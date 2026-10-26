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

package uk.org.hecaton.map;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.ReadOnly;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionalMap;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Immutable maps (load protection, F23): held values are never updated, and are shared by readers. */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ImmutableMapTest extends HecatonTestSupport {

    private static final AtomicInteger OWNER_READS = new AtomicInteger();

    private TestHecatonFactory clientFactory;

    @After
    public void shutdownClients() {
        if (clientFactory != null) {
            clientFactory.terminateAll();
        }
    }

    private static Config config(boolean loadProtection) {
        Config config = smallInstanceConfig();
        config.getLoadProtectionConfig().setEnabled(loadProtection);
        config.addMapConfig(new MapConfig("catalog").setImmutable(true));
        return config;
    }

    @Test
    public void insertsAndRefusesUpdatesOfHeldKeys() {
        IMap<String, String> map = createHecatonInstance(config(true)).getMap("catalog");
        map.put("a", "1");
        map.set("b", "2");
        map.putAll(Map.of("c", "3", "d", "4"));
        map.put("e", "5", 1, TimeUnit.MINUTES);
        map.put("f", "6", 1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
        assertThat(map.putIfAbsent("a", "other")).isEqualTo("1");

        assertRefused(() -> map.put("a", "other"));
        assertRefused(() -> map.put("a", "other", 1, TimeUnit.MINUTES));
        assertRefused(() -> map.set("a", "other"));
        assertRefused(() -> map.putTransient("a", "other", 0, TimeUnit.SECONDS));
        assertRefused(() -> map.tryPut("a", "other", 1, TimeUnit.SECONDS));
        assertRefused(() -> map.putAll(Map.of("a", "other")));
        assertRefused(() -> map.replace("a", "other"));
        assertRefused(() -> map.replace("a", "1", "other"));

        assertThat((Map<String, String>) map).containsEntry("a", "1").containsEntry("b", "2").containsEntry("c", "3")
                .containsEntry("d", "4").containsEntry("e", "5").containsEntry("f", "6").hasSize(6);
    }

    @Test
    public void removesByKeyAndInsertsAgain() {
        IMap<String, String> map = createHecatonInstance(config(true)).getMap("catalog");
        map.putAll(Map.of("a", "1", "b", "2", "c", "3", "d", "4"));

        assertThat(map.remove("a")).isEqualTo("1");
        assertThat(map.remove("b", "other")).isFalse();
        assertThat(map.remove("b", "2")).isTrue();
        map.delete("c");
        assertThat(map.evict("d")).isTrue();
        assertThat(map.isEmpty()).isTrue();

        // a key that is no longer held can be inserted again, with another value
        map.put("a", "again");
        assertThat(map.get("a")).isEqualTo("again");
        assertRefused(() -> map.put("a", "other"));

        map.put("b", "2");
        map.evictAll();
        assertThat(map.isEmpty()).isTrue();
        map.put("c", "3");
        map.clear();
        assertThat(map.isEmpty()).isTrue();
    }

    @Test
    public void expiresByTimeToLive() {
        Config config = config(true);
        config.getMapConfig("catalog").setTimeToLiveSeconds(1);
        IMap<String, String> map = createHecatonInstance(config).getMap("catalog");
        map.put("configured", "1");
        map.put("own", "2", 1, TimeUnit.SECONDS);
        map.put("set", "3", 1, TimeUnit.HOURS);
        map.setTtl("set", 1, TimeUnit.SECONDS);
        assertTrueEventually(() -> assertThat(map.isEmpty()).isTrue());
        map.put("configured", "again");
        assertThat(map.get("configured")).isEqualTo("again");
    }

    @Test
    public void evictsByLeastRecentlyUsed() {
        Config config = config(true);
        config.getMapConfig("catalog").setEvictionConfig(new EvictionConfig().setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_PARTITION).setSize(1));
        HecatonInstance member = createHecatonInstance(config);
        IMap<String, String> map = member.getMap("catalog");
        int partitionId = member.getPartitionService().getPartition("first").getPartitionId();
        map.put("first", "1");
        map.put(generateKeyForPartition(member, partitionId), "2");
        assertThat(map.containsKey("first")).isFalse();
        map.put("first", "again");
        assertThat(map.get("first")).isEqualTo("again");
    }

    @Test
    public void nearCachesSeeARemovedKeyInsertedAgain() {
        HecatonInstance[] members = createHecatonInstanceFactory(2).newInstances(config(true), 2);
        String key = generateKeyOwnedBy(members[0]);
        IMap<String, String> owner = members[0].getMap("catalog");
        IMap<String, String> reader = members[1].getMap("catalog");
        owner.put(key, "1");
        assertThat(reader.get(key)).isEqualTo("1");

        owner.delete(key);
        assertTrueEventually(() -> assertThat(reader.get(key)).isNull());
        owner.put(key, "2");
        assertTrueEventually(() -> assertThat(reader.get(key)).isEqualTo("2"));
    }

    @Test
    public void putAllRefusesTheWholeBatchOfAPartition() {
        HecatonInstance member = createHecatonInstance(config(true));
        IMap<String, String> map = member.getMap("catalog");
        int partitionId = member.getPartitionService().getPartition("held").getPartitionId();
        String fresh = generateKeyForPartition(member, partitionId);
        map.put("held", "1");
        // both keys are in one partition: the held key refuses the batch before the fresh key is inserted
        assertRefused(() -> map.putAll(Map.of("held", "2", fresh, "2")));
        assertThat(map.get("held")).isEqualTo("1");
        assertThat(map.containsKey(fresh)).isFalse();
    }

    @Test
    public void entryProcessorsMayReadInsertAndRemoveButNotUpdate() {
        IMap<String, String> map = createHecatonInstance(config(true)).getMap("catalog");
        map.put("a", "1");

        String read = map.executeOnKey("a", entry -> entry.getValue() + "!");
        assertThat(read).isEqualTo("1!");
        assertThat(map.executeOnKey("a", new ReadingProcessor())).isEqualTo("1");
        map.executeOnKey("new", entry -> entry.setValue("inserted"));
        assertThat(map.get("new")).isEqualTo("inserted");

        assertRefused(() -> map.executeOnKey("a", entry -> entry.setValue("other")));
        assertRefused(() -> map.executeOnEntries(entry -> entry.setValue("other")));
        assertThat(map.get("a")).isEqualTo("1");

        map.executeOnKey("a", entry -> entry.setValue(null));
        assertThat(map.containsKey("a")).isFalse();
    }

    @Test
    public void transactionsMayInsertAndRemoveButNotUpdate() {
        HecatonInstance member = createHecatonInstance(config(true));
        member.getMap("catalog").put("held", "1");

        TransactionContext insert = member.newTransactionContext();
        insert.beginTransaction();
        insert.<String, String>getMap("catalog").put("new", "1");
        insert.<String, String>getMap("catalog").put("expiring", "1", 1, TimeUnit.HOURS);
        insert.commitTransaction();
        assertThat(member.getMap("catalog").get("new")).isEqualTo("1");
        assertThat(member.getMap("catalog").get("expiring")).isEqualTo("1");

        TransactionContext remove = member.newTransactionContext();
        remove.beginTransaction();
        remove.<String, String>getMap("catalog").remove("new");
        remove.<String, String>getMap("catalog").delete("expiring");
        remove.commitTransaction();
        // the member's near cache, which the transaction shares with the map proxy, no longer holds them
        assertThat(member.getMap("catalog").containsKey("new")).isFalse();
        assertThat(member.getMap("catalog").containsKey("expiring")).isFalse();

        for (TransactionalChange change : List.<TransactionalChange>of(
                map -> map.put("held", "2"), map -> map.set("held", "2"), map -> map.replace("held", "2"),
                map -> map.replace("held", "1", "2"))) {
            TransactionContext context = member.newTransactionContext();
            context.beginTransaction();
            TransactionalMap<String, String> map = context.getMap("catalog");
            assertRefused(() -> change.apply(map));
            context.rollbackTransaction();
        }
        assertThat(member.getMap("catalog").get("held")).isEqualTo("1");
        // the refused transactions released their locks
        assertThat(member.getMap("catalog").isLocked("held")).isFalse();
    }

    @Test
    public void notImmutableWithLoadProtectionOff() {
        IMap<String, String> map = createHecatonInstance(config(false)).getMap("catalog");
        map.put("a", "1");
        map.put("a", "2");
        map.remove("a");
        assertThat(map.isEmpty()).isTrue();
    }

    @Test
    public void refusesFormatsThatWouldCopyValues() {
        Config objectFormat = config(true);
        objectFormat.getMapConfig("catalog").setInMemoryFormat(InMemoryFormat.OBJECT);
        assertThatThrownBy(() -> createHecatonInstance(objectFormat).getMap("catalog"))
                .isInstanceOf(InvalidConfigurationException.class).hasMessageContaining("BINARY");
    }

    @Test
    public void membersShareOneObjectAndOneRemoteReadPerKey() throws Exception {
        HecatonInstance[] members = createHecatonInstanceFactory(2).newInstances(config(true), 2);
        HecatonInstance owner = members[0];
        HecatonInstance reader = members[1];
        String key = generateKeyOwnedBy(owner);
        owner.getMap("catalog").put(key, new byte[1 << 20]);
        owner.getMap("catalog").addInterceptor(new CountingInterceptor());
        OWNER_READS.set(0);

        IMap<String, byte[]> map = reader.getMap("catalog");
        int readers = 32;
        CyclicBarrier start = new CyclicBarrier(readers);
        ExecutorService threads = Executors.newFixedThreadPool(readers);
        try {
            List<Future<byte[]>> reads = new ArrayList<>();
            for (int i = 0; i < readers; i++) {
                boolean async = i % 2 == 0;
                reads.add(threads.submit(() -> {
                    start.await();
                    return async ? map.getAsync(key).toCompletableFuture().get() : map.get(key);
                }));
            }
            byte[] first = reads.get(0).get();
            for (Future<byte[]> read : reads) {
                assertThat(read.get()).isSameAs(first);
            }
            assertThat(map.get(key)).isSameAs(first);
        } finally {
            threads.shutdownNow();
        }
        assertThat(OWNER_READS.get()).isEqualTo(1);
    }

    @Test
    public void absentKeysAreNotCached() {
        HecatonInstance[] members = createHecatonInstanceFactory(2).newInstances(config(true), 2);
        String key = generateKeyOwnedBy(members[0]);
        IMap<String, String> reader = members[1].getMap("catalog");
        assertThat(reader.get(key)).isNull();
        members[0].getMap("catalog").put(key, "inserted");
        // visible at once: the earlier miss was not cached
        assertThat(reader.get(key)).isEqualTo("inserted");
    }

    @Test
    public void clientsGetTheImmutableEntryException() {
        clientFactory = new TestHecatonFactory();
        clientFactory.newHecatonInstance(config(true));
        IMap<String, String> map = clientFactory.newHecatonClient().getMap("catalog");
        map.put("a", "1");
        assertThatThrownBy(() -> map.put("a", "2")).isInstanceOf(ImmutableEntryException.class)
                .hasMessageContaining("catalog");
        assertThat(map.get("a")).isEqualTo("1");
    }

    private static void assertRefused(Runnable operation) {
        assertThatThrownBy(operation::run).isInstanceOf(ImmutableEntryException.class)
                .hasMessageContaining("'catalog' is immutable");
    }

    @FunctionalInterface
    private interface TransactionalChange {
        void apply(TransactionalMap<String, String> map);
    }

    private static class ReadingProcessor implements EntryProcessor<String, String, String>, ReadOnly {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public String process(Map.Entry<String, String> entry) {
            return entry.getValue();
        }
    }

    /** Counts the reads that run on the key's owner. */
    private static class CountingInterceptor implements MapInterceptor {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public Object interceptGet(Object value) {
            OWNER_READS.incrementAndGet();
            return null;
        }

        @Override
        public void afterGet(Object value) {
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return null;
        }

        @Override
        public void afterPut(Object value) {
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object value) {
        }
    }
}
