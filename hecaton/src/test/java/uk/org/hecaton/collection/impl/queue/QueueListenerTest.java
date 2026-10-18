/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.collection.impl.queue.model.VersionedObject;
import uk.org.hecaton.collection.impl.queue.model.VersionedObjectComparator;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ItemListenerConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.core.DistributedObjectEvent;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueListenerTest extends HecatonTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Test
    public void testListener_withEvictionViaTTL() throws Exception {
        Config config = getConfig();
        config.getQueueConfig("queueWithTTL").setEmptyQueueTtl(0);
        HecatonInstance hz = createHecatonInstance(config);

        final CountDownLatch latch = new CountDownLatch(2);
        hz.addDistributedObjectListener(new DistributedObjectListener() {
            @Override
            public void distributedObjectCreated(DistributedObjectEvent event) {
                latch.countDown();
            }

            @Override
            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                latch.countDown();
            }
        });

        IQueue<VersionedObject<String>> queue = hz.getQueue("queueWithTTL");
        assertTrue(queue.offer(new VersionedObject<>("item")));
        queue.poll();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testConfigListenerRegistration() throws Exception {
        Config config = getConfig();
        String name = "queue";
        QueueConfig queueConfig = config.getQueueConfig(name);
        CountdownItemListener listener = new CountdownItemListener(1, 1);
        ItemListenerConfig itemListenerConfig = new ItemListenerConfig(listener, true);
        queueConfig.addItemListenerConfig(itemListenerConfig);
        HecatonInstance instance = createHecatonInstance(config);

        IQueue<VersionedObject<String>> queue = instance.getQueue(name);
        assertTrue(queue.offer(new VersionedObject<>("item")));
        queue.poll();

        assertTrue(listener.added.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testItemListener_addedToQueueConfig_Issue366() throws Exception {
        String queueName = "Q";
        int totalQueuePut = 2000;
        CountdownItemListener countdownItemListener = new CountdownItemListener(totalQueuePut, 0);

        Config config = getConfig();
        ItemListenerConfig itemListenerConfig = new ItemListenerConfig()
                .setImplementation(countdownItemListener)
                .setIncludeValue(true);

        config.getQueueConfig(queueName)
              .addItemListenerConfig(itemListenerConfig);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance instance = factory.newHecatonInstance(config);
        IQueue<VersionedObject<Integer>> queue = instance.getQueue(queueName);
        for (int i = 0; i < totalQueuePut / 2; i++) {
            queue.put(new VersionedObject<>(i));
        }
        HecatonInstance second = factory.newHecatonInstance(config);
        assertTrueEventually(() -> {
            EventService eventService1 = getNodeEngineImpl(instance).getEventService();
            EventService eventService2 = getNodeEngineImpl(second).getEventService();
            assertEquals(2, eventService1.getRegistrations(QueueService.SERVICE_NAME, queueName).size());
            assertEquals(2, eventService2.getRegistrations(QueueService.SERVICE_NAME, queueName).size());
        });
        for (int i = 0; i < totalQueuePut / 4; i++) {
            queue.put(new VersionedObject<>(i));
        }
        assertOpenEventually(countdownItemListener.added);
    }

    @Test
    public void testListeners() throws Exception {
        int totalQueuePut = 2000;
        String queueName = randomString();
        HecatonInstance instance = createHecatonInstance();
        IQueue<VersionedObject<String>> queue = instance.getQueue(queueName);

        TestItemListener listener = new TestItemListener(totalQueuePut);
        UUID listenerId = queue.addItemListener(listener, true);

        for (int i = 0; i < totalQueuePut / 2; i++) {
            assertTrue(queue.offer(new VersionedObject<>("item-" + i, i)));
        }
        for (int i = 0; i < totalQueuePut / 2; i++) {
            queue.poll();
        }
        assertTrue(listener.latch.await(5, TimeUnit.SECONDS));

        queue.removeItemListener(listenerId);
        assertTrue(queue.offer(new VersionedObject<>("item-a")));
        queue.poll();
        assertTrue(listener.notCalled.get());
    }

    private static class TestItemListener implements ItemListener<VersionedObject<String>> {

        CountDownLatch latch;
        AtomicBoolean notCalled;
        int offer;
        int poll;

        TestItemListener(int count) {
            this.latch = new CountDownLatch(count);
            this.notCalled = new AtomicBoolean(true);
        }

        @Override
        public void itemAdded(ItemEvent<VersionedObject<String>> item) {
            int id = offer++;
            if (item.getItem().equals(new VersionedObject<>("item-" + id, id))) {
                latch.countDown();
            } else {
                notCalled.set(false);
            }
        }

        @Override
        public void itemRemoved(ItemEvent<VersionedObject<String>> item) {
            int id = poll++;
            if (item.getItem().equals(new VersionedObject<>("item-" + id, id))) {
                latch.countDown();
            } else {
                notCalled.set(false);
            }
        }
    }

    private static class CountdownItemListener implements ItemListener<VersionedObject<String>> {

        public CountDownLatch added;
        public CountDownLatch removed;

        CountdownItemListener(int expectedAdded, int expectedRemoved) {
            added = new CountDownLatch(expectedAdded);
            removed = new CountDownLatch(expectedRemoved);
        }

        @Override
        public void itemAdded(ItemEvent<VersionedObject<String>> itemEvent) {
            added.countDown();
        }

        @Override
        public void itemRemoved(ItemEvent<VersionedObject<String>> itemEvent) {
            removed.countDown();
        }
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        return config;
    }
}
