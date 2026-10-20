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

package uk.org.hecaton.internal.jmx;

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.ITopic;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

/**
 * This is the test that just tests if the mbeans get created. Tests in this class only exist for types that don't have
 * a specific test in this package. See LockMBeanTest for a specific test.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MBeanTest extends HecatonTestSupport {

    private MBeanDataHolder holder;

    @Before
    public void setUp() throws Exception {
        holder = new MBeanDataHolder(createHecatonInstanceFactory(1));
    }

    @Test
    public void testMap() throws Exception {
        IMap map = holder.getHz().getMap("map");
        map.size();

        holder.assertMBeanExistEventually("IMap", map.getName());
    }

    @Test
    public void testMultiMap() {
        MultiMap map = holder.getHz().getMultiMap("multimap");
        map.size();

        holder.assertMBeanExistEventually("MultiMap", map.getName());
    }

    @Test
    public void testTopic() {
        ITopic<String> topic = holder.getHz().getTopic("topic");
        topic.publish("foo");

        holder.assertMBeanExistEventually("ITopic", topic.getName());
    }

    @Test
    public void testList() throws Exception {
        IList list = holder.getHz().getList("list");
        list.size();

        holder.assertMBeanExistEventually("IList", list.getName());
    }

    @Test
    public void testSet() throws Exception {
        ISet set = holder.getHz().getSet("set");
        set.size();

        holder.assertMBeanExistEventually("ISet", set.getName());
    }

    @Test
    public void testQueue() throws Exception {
        IQueue queue = holder.getHz().getQueue("queue");
        queue.size();

        holder.assertMBeanExistEventually("IQueue", queue.getName());
    }

    @Test
    public void testExecutor() throws Exception {
        IExecutorService executor = holder.getHz().getExecutorService("executor");
        executor.submit(new DummyRunnable()).get();

        holder.assertMBeanExistEventually("IExecutorService", executor.getName());
    }

    @Test
    public void testReplicatedMap() {
        String replicatedMapName = randomString();
        ReplicatedMap replicatedMap = holder.getHz().getReplicatedMap(replicatedMapName);
        replicatedMap.size();

        holder.assertMBeanExistEventually("ReplicatedMap", replicatedMap.getName());
    }

    private static class DummyRunnable implements Runnable, Serializable {

        @Override
        public void run() {
        }
    }
}
