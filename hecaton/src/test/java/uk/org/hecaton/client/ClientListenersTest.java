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

package uk.org.hecaton.client;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.MergeOperation;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpiryMetadata;
import uk.org.hecaton.map.listener.EntryMergedListener;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.ITopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientListenersTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    HecatonInstance client;
    HecatonInstance server;

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        ClientConfig config = new ClientConfig();
        config.getSerializationConfig().addPortableFactory(5, classId -> {
            if (classId == 6) {
                return new ClientRegressionWithMockNetworkTest.SamplePortable();
            }
            return null;
        });

        config.addListenerConfig(new ListenerConfig("uk.org.hecaton.client.ClientListenersTest$StaticListener"));

        server = hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient(config);
    }

    @Test
    public void testEntryListener_withPortableNotRegisteredInNode() {
        final IMap<Object, Object> map = client.getMap(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener(new EntryAdapter<>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        }, true);

        map.put(1, new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testEntryMergeListener_withPortableNotRegisteredInNode() {
        final IMap<Object, Object> map = client.getMap(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener((EntryMergedListener<Object, Object>) event -> latch.countDown(), true);

        Node node = getNode(server);
        NodeEngineImpl nodeEngine = node.nodeEngine;
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        SerializationService serializationService = getSerializationService(server);
        Data key = serializationService.toData(1);
        Data value = serializationService.toData(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        SplitBrainMergeTypes.MapMergeTypes mergingEntry = createMergingEntry(serializationService, key, value,
                Mockito.mock(Record.class), ExpiryMetadata.NULL);
        Operation op = new MergeOperation(map.getName(), Collections.singletonList(mergingEntry),
                new PassThroughMergePolicy<>(), false);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);

        assertOpenEventually(latch);
    }

    @Test
    public void testItemListener_withPortableNotRegisteredInNode() {
        final IQueue<Object> queue = client.getQueue(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        queue.addItemListener(new ItemListener<>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        queue.offer(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testSetListener_withPortableNotRegisteredInNode() {
        final ISet<Object> set = client.getSet(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        set.addItemListener(new ItemListener<>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        set.add(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testListListener_withPortableNotRegisteredInNode() {
        final IList<Object> list = client.getList(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        list.addItemListener(new ItemListener<>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        list.add(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testTopic_withPortableNotRegisteredInNode() {
        final ITopic<Object> topic = client.getTopic(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        topic.addMessageListener(message -> latch.countDown());

        topic.publish(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testLifecycleListener_registeredViaClassName() {
        assertTrue(StaticListener.CALLED_AT_LEAST_ONCE.get());
    }

    public static class StaticListener implements LifecycleListener {

        private static final AtomicBoolean CALLED_AT_LEAST_ONCE = new AtomicBoolean();

        @Override
        public void stateChanged(LifecycleEvent event) {
            CALLED_AT_LEAST_ONCE.set(true);
        }
    }
}
