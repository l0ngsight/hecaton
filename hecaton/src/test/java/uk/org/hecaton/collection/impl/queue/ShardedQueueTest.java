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

package uk.org.hecaton.collection.impl.queue;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;

/** Sharded queues (load protection, F23): one logical queue over several partitions. */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ShardedQueueTest extends HecatonTestSupport {

    private TestHecatonFactory factory;

    @After
    public void shutdown() {
        if (factory != null) {
            factory.terminateAll();
        }
    }

    private static Config config(int shards) {
        Config config = smallInstanceConfig();
        config.getLoadProtectionConfig().setEnabled(true);
        config.addQueueConfig(new QueueConfig("jobs").setShardCount(shards));
        return config;
    }

    @Test
    public void membersAndClientsShareOneLogicalQueueOverItsShards() {
        factory = new TestHecatonFactory();
        HecatonInstance[] members = {factory.newHecatonInstance(config(8)), factory.newHecatonInstance(config(8)),
                factory.newHecatonInstance(config(8))};
        HecatonInstance client = factory.newHecatonClient();
        IQueue<Integer> fromMember = members[0].getQueue("jobs");
        IQueue<Integer> fromClient = client.getQueue("jobs");
        assertThat(fromMember).isInstanceOf(ShardedQueueProxy.class);

        for (int i = 0; i < 100; i++) {
            assertThat(i % 2 == 0 ? fromMember.offer(i) : fromClient.offer(i)).isTrue();
        }
        assertThat(fromMember.size()).isEqualTo(100);
        assertThat(fromClient.size()).isEqualTo(100);

        // the items are spread over the shards, and the shards over the members
        ShardedQueue sharded = ((ShardedQueueProxy<?>) fromMember).getShardedQueue();
        Set<Integer> partitions = new HashSet<>();
        Set<Object> owners = new HashSet<>();
        for (String shard : sharded.getShardNames()) {
            assertThat(members[0].getQueue(shard).size()).isPositive();
            // queues place themselves by the partition key of their name: the part after '@'
            Object partitionKey = StringPartitioningStrategy.getPartitionKey(shard);
            partitions.add(members[0].getPartitionService().getPartition(partitionKey).getPartitionId());
            owners.add(members[0].getPartitionService().getPartition(partitionKey).getOwner());
        }
        assertThat(partitions).hasSize(8);
        assertThat(owners).hasSizeGreaterThan(1);

        Set<Integer> polled = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Integer item = i % 2 == 0 ? fromClient.poll() : members[1].<Integer>getQueue("jobs").poll();
            assertThat(item).isNotNull();
            polled.add(item);
        }
        assertThat(polled).hasSize(100);
        assertThat(fromClient.poll()).isNull();
        assertThat(fromMember.isEmpty()).isTrue();
    }

    @Test
    public void aWaitingTakeGetsAnItemOfferedToAnyShard() throws Exception {
        factory = new TestHecatonFactory();
        HecatonInstance member = factory.newHecatonInstance(config(4));
        factory.newHecatonInstance(config(4));
        HecatonInstance client = factory.newHecatonClient();

        Future<Integer> memberTake = spawn(() -> member.<Integer>getQueue("jobs").take());
        Future<Integer> clientTake = spawn(() -> client.<Integer>getQueue("jobs").take());
        sleepMillis(200);
        assertThat(memberTake.isDone()).isFalse();
        assertThat(clientTake.isDone()).isFalse();

        for (int i = 0; i < 2; i++) {
            member.getQueue("jobs").offer(i);
        }
        assertThat(Set.of(memberTake.get(10, TimeUnit.SECONDS), clientTake.get(10, TimeUnit.SECONDS)))
                .containsExactlyInAnyOrder(0, 1);
        assertThat(client.<Integer>getQueue("jobs").poll(100, TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    public void bulkOperationsAndListenersSpanTheShards() {
        factory = new TestHecatonFactory();
        HecatonInstance member = factory.newHecatonInstance(config(4));
        HecatonInstance client = factory.newHecatonClient();
        for (IQueue<String> queue : List.<IQueue<String>>of(member.getQueue("jobs"), client.getQueue("jobs"))) {
            List<ItemEvent<String>> events = new CopyOnWriteArrayList<>();
            ItemListener<String> listener = new ItemListener<>() {
                @Override
                public void itemAdded(ItemEvent<String> item) {
                    events.add(item);
                }

                @Override
                public void itemRemoved(ItemEvent<String> item) {
                }
            };
            var registration = queue.addItemListener(listener, true);

            assertThat(queue.addAll(List.of("a", "b", "c", "d", "e"))).isTrue();
            assertThat(queue.contains("c")).isTrue();
            assertThat(queue.containsAll(List.of("a", "e"))).isTrue();
            assertThat(queue.containsAll(List.of("a", "x"))).isFalse();
            assertThat(queue.remove("c")).isTrue();
            assertThat(queue).containsExactlyInAnyOrder("a", "b", "d", "e");
            assertThat(queue.peek()).isNotNull();
            assertThat(queue.removeAll(List.of("a"))).isTrue();
            List<String> drained = new ArrayList<>();
            assertThat(queue.drainTo(drained, 2)).isEqualTo(2);
            assertThat(queue.drainTo(drained)).isEqualTo(1);
            assertThat(drained).containsExactlyInAnyOrder("b", "d", "e");
            assertThat(queue.isEmpty()).isTrue();
            assertThat(queue.remainingCapacity()).isPositive();

            assertTrueEventually(() -> assertThat(events).hasSize(5));
            assertThat(events).allSatisfy(event -> assertThat(event.getSource()).isEqualTo("jobs"));
            assertThat(queue.removeItemListener(registration)).isTrue();
            queue.offer("x");
            queue.clear();
            assertThat(queue.size()).isZero();
        }
    }

    @Test
    public void existingQueuesBecomeTheShardsOfALogicalQueue() {
        factory = new TestHecatonFactory();
        Config config = smallInstanceConfig();
        config.getLoadProtectionConfig().setEnabled(true);
        config.addQueueConfig(new QueueConfig("jobs").setShardNames(List.of("jobs-0", "jobs-1", "jobs-2")));
        HecatonInstance member = factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient();

        // applications that hash on the client keep using the physical queues
        client.getQueue("jobs-0").offer("old-0");
        client.getQueue("jobs-2").offer("old-2");
        IQueue<String> logical = client.getQueue("jobs");
        assertThat(logical.size()).isEqualTo(2);
        logical.offer("new");
        assertThat(member.getQueue("jobs-0").size() + member.getQueue("jobs-1").size() + member.getQueue("jobs-2").size())
                .isEqualTo(3);
        assertThat(Set.of(logical.poll(), logical.poll(), logical.poll())).containsExactlyInAnyOrder("old-0", "old-2", "new");
    }

    @Test
    public void notShardedWithLoadProtectionOff() {
        Config config = config(4);
        config.getLoadProtectionConfig().setEnabled(false);
        HecatonInstance member = createHecatonInstance(config);
        IQueue<String> queue = member.getQueue("jobs");
        assertThat(queue).isInstanceOf(QueueProxyImpl.class);
    }

    @Test
    public void noAcknowledgedItemIsLostWhenAMemberDies() {
        factory = new TestHecatonFactory();
        HecatonInstance[] members = {factory.newHecatonInstance(config(6)), factory.newHecatonInstance(config(6)),
                factory.newHecatonInstance(config(6))};
        IQueue<Integer> queue = members[0].getQueue("jobs");
        for (int i = 0; i < 300; i++) {
            assertThat(queue.offer(i)).isTrue();
        }

        members[2].getLifecycleService().terminate();
        waitAllForSafeState(members[0], members[1]);

        Set<Integer> polled = new HashSet<>();
        Integer item;
        while ((item = members[1].<Integer>getQueue("jobs").poll()) != null) {
            polled.add(item);
        }
        assertThat(polled).hasSize(300);
    }

    @Test
    public void anInterruptedTakeLosesNoItem() throws Exception {
        HecatonInstance member = createHecatonInstance(config(2));
        IQueue<String> queue = member.getQueue("jobs");
        Thread taker = new Thread(() -> {
            try {
                queue.take();
            } catch (InterruptedException expected) {
                // interrupted while waiting
            }
        });
        taker.start();
        sleepMillis(200);
        taker.interrupt();
        taker.join(5_000);

        queue.offer("kept");
        sleepMillis(300);
        assertThat(queue.poll(5, TimeUnit.SECONDS)).isEqualTo("kept");
        assertThat(getNodeEngineImpl(member).getOperationParker()).isNotNull();
    }
}
