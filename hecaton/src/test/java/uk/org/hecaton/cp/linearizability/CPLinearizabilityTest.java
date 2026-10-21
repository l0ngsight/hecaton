/*
 * Copyright (c) 2026, The Hecaton Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.org.hecaton.cp.linearizability;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.IAtomicLong;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.cp.internal.RaftServiceDataSerializerHook;
import uk.org.hecaton.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsToAddresses;
import static uk.org.hecaton.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs CPMap and IAtomicLong operations from members and a client while a nemesis partitions the
 * Raft traffic, isolates the leader and replaces CP members, records the history and checks that
 * it is linearizable. The run lasts {@code hecaton.test.linearizability.seconds} seconds (default 40).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class CPLinearizabilityTest extends HecatonRaftTestSupport {

    private static final int CP_MEMBERS = 5;
    private static final int WORKERS = 6;
    private static final long OP_TIMEOUT_SECONDS = 5;
    private static final List<String> KEYS = List.of("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7");
    /**
     * Pause between a worker's operations. The checker needs memory quadratic in the length of a
     * key's history, so the workers are throttled to keep each key at a few thousand operations.
     */
    private static final int MAX_PAUSE_MILLIS = 20;
    /** Raft RPCs between the members of a group: votes, appends and snapshots. */
    private static final List<Integer> RAFT_RPCS = List.of(
            RaftServiceDataSerializerHook.PRE_VOTE_REQUEST_OP, RaftServiceDataSerializerHook.PRE_VOTE_RESPONSE_OP,
            RaftServiceDataSerializerHook.VOTE_REQUEST_OP, RaftServiceDataSerializerHook.VOTE_RESPONSE_OP,
            RaftServiceDataSerializerHook.APPEND_REQUEST_OP, RaftServiceDataSerializerHook.APPEND_SUCCESS_RESPONSE_OP,
            RaftServiceDataSerializerHook.APPEND_FAILURE_RESPONSE_OP, RaftServiceDataSerializerHook.INSTALL_SNAPSHOT_OP);

    private static final ILogger LOGGER = Logger.getLogger(CPLinearizabilityTest.class);

    private final List<HecatonInstance> live = new CopyOnWriteArrayList<>();
    private final Map<HecatonInstance, CPMap<String, String>> maps = new ConcurrentHashMap<>();
    private final Map<HecatonInstance, IAtomicLong> longs = new ConcurrentHashMap<>();
    private final ExecutorService callers = Executors.newCachedThreadPool();
    private final History mapHistory = new History();
    private final History longHistory = new History();
    private HecatonInstance client;
    private CPGroupId groupId;
    private volatile boolean running = true;

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return new TestHecatonFactory();
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig()
              .setSessionHeartbeatIntervalSeconds(1)
              .setSessionTimeToLiveSeconds(10)
              .setMissingCPMemberAutoRemovalSeconds(10)
              // without it an invocation whose outcome is unknown is retried and may apply twice
              .setFailOnIndeterminateOperationState(true);
        return config;
    }

    @After
    public void tearDown() {
        running = false;
        callers.shutdownNow();
        factory.terminateAll();
    }

    @Test
    public void historyIsLinearizable() throws Exception {
        Collections.addAll(live, newInstances(CP_MEMBERS, CP_MEMBERS, 1));
        client = ((TestHecatonFactory) factory).newHecatonClient();
        CPMap<String, String> map = client.getCPSubsystem().getMap("linearizable");
        groupId = ((CPMapProxy<?, ?>) live.get(0).getCPSubsystem().getMap("linearizable")).getGroupId();
        map.set("warmup", "x");

        long seconds = Long.getLong("hecaton.test.linearizability.seconds", 40);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < WORKERS; i++) {
            int id = i;
            Thread t = new Thread(() -> work(id, deadline), "linearizability-worker-" + i);
            t.start();
            workers.add(t);
        }
        Nemesis nemesis = new Nemesis(new Random(seconds ^ System.nanoTime()));
        nemesis.run(deadline);
        for (Thread t : workers) {
            t.join(TimeUnit.SECONDS.toMillis(OP_TIMEOUT_SECONDS * 4));
        }
        running = false;

        // after healing, the cluster serves requests again
        for (String key : KEYS) {
            mapHistory.record(key, new MapKeyModel.Input(MapKeyModel.Type.GET, null, null), true,
                    () -> client.getCPSubsystem().<String, String>getMap("linearizable").get(key));
        }

        List<Operation> mapOps = mapHistory.operations();
        List<Operation> longOps = longHistory.operations();
        long known = mapOps.stream().filter(Operation::outcomeKnown).count()
                + longOps.stream().filter(Operation::outcomeKnown).count();
        LOGGER.info("Linearizability run: " + mapOps.size() + " map and " + longOps.size() + " atomic long operations, "
                + (mapHistory.unknownCount() + longHistory.unknownCount()) + " with unknown outcome; faults: "
                + nemesis.faults);
        assertTrue("too few operations completed: " + known, known >= 200);

        assertLinearizable(new MapKeyModel(), mapOps);
        assertLinearizable(new AtomicLongModel(), longOps);
    }

    private static <S> void assertLinearizable(Model<S> model, List<Operation> ops) {
        List<Object> failed = LinearizabilityChecker.nonLinearizableKeys(model, ops);
        if (!failed.isEmpty()) {
            Object key = failed.get(0);
            List<Operation> keyOps = ops.stream().filter(op -> op.key().equals(key))
                    .sorted(Comparator.comparingLong(Operation::call)).toList();
            throw new AssertionError("History of " + key + " is not linearizable (" + keyOps.size()
                    + " operations), shortest failing prefix ends with:\n" + describe(failingPrefix(model, keyOps)));
        }
    }

    /** The shortest prefix (by call time) of the history that is not linearizable. */
    private static <S> List<Operation> failingPrefix(Model<S> model, List<Operation> ops) {
        int lo = 1;
        int hi = ops.size();
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            if (LinearizabilityChecker.isLinearizable(model, ops.subList(0, mid))) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return ops.subList(Math.max(0, lo - 30), lo);
    }

    private void work(int worker, long deadline) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // values this worker has seen, so compare-and-sets often succeed
        long seenLong = 0;
        Map<String, String> seen = new ConcurrentHashMap<>();
        while (running && System.nanoTime() < deadline) {
            sleepMillis(random.nextInt(MAX_PAUSE_MILLIS));
            HecatonInstance instance = pickCaller(random);
            if (random.nextInt(8) == 0) {
                long expected = seenLong;
                AtomicLongModel.Input input = switch (random.nextInt(3)) {
                    case 0 -> new AtomicLongModel.Input(AtomicLongModel.Type.GET, 0, 0);
                    case 1 -> new AtomicLongModel.Input(AtomicLongModel.Type.ADD_AND_GET, 1 + random.nextInt(3), 0);
                    default -> new AtomicLongModel.Input(AtomicLongModel.Type.COMPARE_AND_SET, expected + 10, expected);
                };
                Object out = longHistory.record("counter", input, input.type() == AtomicLongModel.Type.GET,
                        () -> call(() -> apply(atomicLong(instance), input)));
                if (out instanceof Long l) {
                    seenLong = l;
                }
            } else {
                String key = KEYS.get(random.nextInt(KEYS.size()));
                String value = "w" + worker + "-" + random.nextInt(1_000_000);
                MapKeyModel.Type type = pickType(random);
                boolean writes = type != MapKeyModel.Type.GET && type != MapKeyModel.Type.REMOVE
                        && type != MapKeyModel.Type.DELETE;
                String expected = type == MapKeyModel.Type.COMPARE_AND_SET ? seen.getOrDefault(key, "none") : null;
                MapKeyModel.Input input = new MapKeyModel.Input(type, writes ? value : null, expected);
                Object out = mapHistory.record(key, input, type == MapKeyModel.Type.GET,
                        () -> call(() -> apply(map(instance), key, input)));
                if (out instanceof String s) {
                    seen.put(key, s);
                }
            }
        }
    }

    private static MapKeyModel.Type pickType(ThreadLocalRandom random) {
        int r = random.nextInt(100);
        if (r < 35) {
            return MapKeyModel.Type.GET;
        } else if (r < 50) {
            return MapKeyModel.Type.SET;
        } else if (r < 60) {
            return MapKeyModel.Type.PUT;
        } else if (r < 65) {
            return MapKeyModel.Type.PUT_IF_ABSENT;
        } else if (r < 70) {
            return MapKeyModel.Type.REMOVE;
        } else if (r < 75) {
            return MapKeyModel.Type.DELETE;
        }
        return MapKeyModel.Type.COMPARE_AND_SET;
    }

    private static Object apply(CPMap<String, String> map, String key, MapKeyModel.Input input) {
        return switch (input.type()) {
            case GET -> map.get(key);
            case PUT -> map.put(key, input.value());
            case SET -> {
                map.set(key, input.value());
                yield null;
            }
            case PUT_IF_ABSENT -> map.putIfAbsent(key, input.value());
            case REMOVE -> map.remove(key);
            case DELETE -> {
                map.delete(key);
                yield null;
            }
            case COMPARE_AND_SET -> map.compareAndSet(key, input.expected(), input.value());
        };
    }

    private static Object apply(IAtomicLong atomicLong, AtomicLongModel.Input input) {
        return switch (input.type()) {
            case GET -> atomicLong.get();
            case ADD_AND_GET -> atomicLong.addAndGet(input.value());
            case COMPARE_AND_SET -> atomicLong.compareAndSet(input.expected(), input.value());
        };
    }

    /** Runs a call with a time limit; a call that does not finish in time has an unknown outcome. */
    private Object call(Callable<Object> action) throws Exception {
        Future<Object> future = callers.submit(action);
        try {
            return future.get(OP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }

    private HecatonInstance pickCaller(ThreadLocalRandom random) {
        List<HecatonInstance> members = new ArrayList<>(live);
        int i = random.nextInt(members.size() + 1);
        return i == members.size() ? client : members.get(i);
    }

    private CPMap<String, String> map(HecatonInstance instance) {
        return maps.computeIfAbsent(instance, i -> i.getCPSubsystem().getMap("linearizable"));
    }

    private IAtomicLong atomicLong(HecatonInstance instance) {
        return longs.computeIfAbsent(instance, i -> i.getCPSubsystem().getAtomicLong("counter@linearizable"));
    }

    private static String describe(List<Operation> ops) {
        return ops.stream().map(Object::toString).collect(Collectors.joining("\n"));
    }

    /**
     * Injects one fault at a time until the deadline: a Raft partition with a random minority, an
     * isolated leader, or the replacement of a CP member.
     */
    private final class Nemesis {
        private final Random random;
        private final List<String> faults = new ArrayList<>();

        Nemesis(Random random) {
            this.random = random;
        }

        void run(long deadline) throws Exception {
            while (System.nanoTime() < deadline) {
                sleepMillis(1000 + random.nextInt(1000));
                switch (random.nextInt(5)) {
                    case 0, 1 -> partition();
                    case 2, 3 -> isolateLeader();
                    default -> replaceMember();
                }
            }
            heal();
        }

        private void partition() {
            List<HecatonInstance> members = cpMembers();
            Collections.shuffle(members, random);
            int minority = 1 + random.nextInt((members.size() - 1) / 2);
            split(members.subList(0, minority), members.subList(minority, members.size()));
            faults.add("partition(" + minority + "/" + members.size() + ")");
            sleepMillis(2000 + random.nextInt(2000));
            heal();
        }

        private void isolateLeader() {
            List<HecatonInstance> members = cpMembers();
            HecatonInstance leader = getLeaderInstance(members.toArray(new HecatonInstance[0]), groupId);
            List<HecatonInstance> others = new ArrayList<>(members);
            others.remove(leader);
            split(List.of(leader), others);
            faults.add("isolate-leader");
            sleepMillis(2000 + random.nextInt(2000));
            heal();
        }

        /** Terminates a CP member, removes it from the CP Subsystem and promotes a new member. */
        private void replaceMember() throws Exception {
            List<HecatonInstance> members = cpMembers();
            HecatonInstance victim = members.get(random.nextInt(members.size()));
            UUID uuid = victim.getCPSubsystem().getLocalCPMember().getUuid();
            live.remove(victim);
            victim.getLifecycleService().terminate();
            HecatonInstance survivor = live.get(0);
            assertTrueEventually(() -> {
                survivor.getCPSubsystem().getCPSubsystemManagementService().removeCPMember(uuid)
                        .toCompletableFuture().get();
                Collection<CPMember> cpMembers = survivor.getCPSubsystem().getCPSubsystemManagementService()
                        .getCPMembers().toCompletableFuture().get();
                assertEquals(CP_MEMBERS - 1, cpMembers.size());
            });
            HecatonInstance replacement = factory.newHecatonInstance(createConfig(CP_MEMBERS, CP_MEMBERS));
            replacement.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                       .toCompletableFuture().get();
            live.add(replacement);
            faults.add("replace-member");
        }

        private List<HecatonInstance> cpMembers() {
            List<HecatonInstance> members = new ArrayList<>();
            for (HecatonInstance instance : live) {
                if (instance.getCPSubsystem().getLocalCPMember() != null) {
                    members.add(instance);
                }
            }
            return members;
        }

        private void split(List<HecatonInstance> a, List<HecatonInstance> b) {
            for (HecatonInstance x : a) {
                dropOperationsToAddresses(x, b.stream().map(Accessors::getAddress).toList(),
                        RaftServiceDataSerializerHook.F_ID, RAFT_RPCS);
            }
            for (HecatonInstance x : b) {
                dropOperationsToAddresses(x, a.stream().map(Accessors::getAddress).toList(),
                        RaftServiceDataSerializerHook.F_ID, RAFT_RPCS);
            }
        }

        private void heal() {
            for (HecatonInstance instance : live) {
                resetPacketFiltersFrom(instance);
            }
        }
    }
}
