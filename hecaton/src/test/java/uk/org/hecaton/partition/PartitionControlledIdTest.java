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

package uk.org.hecaton.partition;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.collection.impl.list.ListService;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.PartitioningStrategyConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionControlledIdTest extends HecatonTestSupport {

    private static HecatonInstance[] instances;

    @BeforeClass
    public static void startHecatonInstances() {
        Config config = new Config();
        PartitioningStrategy partitioningStrategy = StringAndPartitionAwarePartitioningStrategy.INSTANCE;
        config.getMapConfig("default")
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig(partitioningStrategy));

        TestHecatonInstanceFactory instanceFactory = new TestHecatonInstanceFactory(4);
        instances = instanceFactory.newInstances(config);

        warmUpPartitions(instances);
    }

    @AfterClass
    public static void killHecatonInstances() {
        HecatonInstanceFactory.terminateAll();
    }

    private HecatonInstance getHecatonInstance(String partitionKey) {
        Partition partition = instances[0].getPartitionService().getPartition(partitionKey);
        Member owner = partition.getOwner();
        assertNotNull(owner);

        HecatonInstance hz = null;
        for (HecatonInstance instance : instances) {
            if (instance.getCluster().getLocalMember().equals(owner)) {
                hz = instance;
                break;
            }
        }
        assertNotNull(hz);
        return hz;
    }

    public NodeEngineImpl getNodeEngine(HecatonInstance hz) {
        Node node = getNode(hz);
        return node.nodeEngine;
    }

    @Test
    public void testRingbuffer() {
        String partitionKey = "hecaton";
        HecatonInstance hz = getHecatonInstance(partitionKey);

        Ringbuffer<String> ringbuffer = hz.getRingbuffer("ringbuffer@" + partitionKey);
        ringbuffer.add("foo");
        assertEquals("ringbuffer@" + partitionKey, ringbuffer.getName());
        assertEquals(partitionKey, ringbuffer.getPartitionKey());

        RingbufferService service = getNodeEngine(hz).getService(RingbufferService.SERVICE_NAME);
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers =
                service.getContainers().get(service.getRingbufferPartitionId(ringbuffer.getName()));
        assertNotNull(partitionContainers);
        assertTrue(partitionContainers.containsKey(RingbufferService.getRingbufferNamespace(ringbuffer.getName())));
    }

    @Test
    public void testQueue() {
        String partitionKey = "hecaton";
        HecatonInstance hz = getHecatonInstance(partitionKey);

        IQueue<String> queue = hz.getQueue("queue@" + partitionKey);
        queue.add("");
        assertEquals("queue@" + partitionKey, queue.getName());
        assertEquals(partitionKey, queue.getPartitionKey());

        QueueService service = getNodeEngine(hz).getService(QueueService.SERVICE_NAME);
        assertTrue(service.containsQueue(queue.getName()));
    }

    @Test
    public void testList() {
        String partitionKey = "hecaton";
        HecatonInstance hz = getHecatonInstance(partitionKey);

        IList<String> list = hz.getList("list@" + partitionKey);
        list.add("");
        assertEquals("list@" + partitionKey, list.getName());
        assertEquals(partitionKey, list.getPartitionKey());

        ListService service = getNodeEngine(hz).getService(ListService.SERVICE_NAME);
        assertTrue(service.getContainerMap().containsKey(list.getName()));
    }

    @Test
    public void testSet() {
        String partitionKey = "hecaton";
        HecatonInstance hz = getHecatonInstance(partitionKey);

        ISet<String> set = hz.getSet("set@" + partitionKey);
        set.add("");
        assertEquals("set@" + partitionKey, set.getName());
        assertEquals(partitionKey, set.getPartitionKey());

        SetService service = getNodeEngine(hz).getService(SetService.SERVICE_NAME);
        assertTrue(service.getContainerMap().containsKey(set.getName()));
    }

    @Test
    public void testObjectWithPartitionKeyAndTask() throws Exception {
        HecatonInstance instance = instances[0];
        IExecutorService executorServices = instance.getExecutorService("executor");
        String partitionKey = "hecaton";
        IList<Object> list = instance.getList("foobar@" + partitionKey);
        list.add("value");
        ContainsListTask task = new ContainsListTask(list.getName());
        Future<Boolean> future = executorServices.submitToKeyOwner(task, list.getPartitionKey());
        assertTrue(future.get());
    }

    private static class ContainsListTask implements Callable<Boolean>, HecatonInstanceAware, Serializable {

        private final String name;

        private transient HecatonInstance hz;

        private ContainsListTask(String name) {
            this.name = name;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.hz = hecatonInstance;
        }

        @Override
        public Boolean call() {
            NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
            ListService service = nodeEngine.getService(ListService.SERVICE_NAME);
            return service.getContainerMap().containsKey(name);
        }
    }

    @Test
    public void testObjectWithPartitionKeyAndMap() throws Exception {
        HecatonInstance instance = instances[0];
        IExecutorService executorServices = instance.getExecutorService("executor");
        String partitionKey = "hecaton";
        String mapKey = "key@" + partitionKey;
        IMap<String, String> map = instance.getMap("map");

        map.put(mapKey, "foobar");

        IList<Object> list = instance.getList("s@" + partitionKey);
        list.add("value");

        ContainsListAndMapEntryTask task = new ContainsListAndMapEntryTask(list.getName(), mapKey);
        Map<Member, Future<Boolean>> futures = executorServices.submitToAllMembers(task);

        int count = 0;
        for (Future<Boolean> future : futures.values()) {
            count += future.get() ? 1 : 0;
        }

        assertEquals(1, count);
    }

    private static class ContainsListAndMapEntryTask implements Callable<Boolean>, HecatonInstanceAware, Serializable {

        private final String name;
        private final String mapKey;

        private transient HecatonInstance hz;

        private ContainsListAndMapEntryTask(String name, String mapKey) {
            this.name = name;
            this.mapKey = mapKey;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.hz = hecatonInstance;
        }

        @Override
        public Boolean call() {
            NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
            ListService service = nodeEngine.getService(ListService.SERVICE_NAME);

            IMap map = hz.getMap("map");
            return map.localKeySet().contains(mapKey) && service.getContainerMap().containsKey(name);
        }
    }
}
