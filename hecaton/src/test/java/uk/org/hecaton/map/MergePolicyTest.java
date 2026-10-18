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

package uk.org.hecaton.map;

import uk.org.hecaton.cluster.MembershipEvent;
import uk.org.hecaton.cluster.MembershipListener;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.spi.merge.HigherHitsMergePolicy;
import uk.org.hecaton.spi.merge.LatestUpdateMergePolicy;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.PutIfAbsentMergePolicy;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class MergePolicyTest extends HecatonTestSupport {

    @Before
    @After
    public void killAllHecatonInstances() {
        HecatonInstanceFactory.terminateAll();
    }

    @Test
    public void testLatestUpdateMapMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(LatestUpdateMergePolicy.class.getName(), mapName);
        HecatonInstance h1 = Hecaton.newHecatonInstance(config);
        HecatonInstance h2 = Hecaton.newHecatonInstance(config);

        warmUpPartitions(h1, h2);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);

        CountDownLatch mergeBlockingLatch = new CountDownLatch(1);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1, mergeBlockingLatch);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.memberRemovedLatch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        map1.put("key1", "value");
        // prevent updating at the same time
        sleepAtLeastMillis(1000);
        map2.put("key1", "LatestUpdatedValue");
        map2.put("key2", "value2");
        // prevent updating at the same time
        sleepAtLeastMillis(1000);
        map1.put("key2", "LatestUpdatedValue2");

        // allow merge process to continue
        mergeBlockingLatch.countDown();

        assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        assertClusterSizeEventually(2, h1, h2);

        IMap<Object, Object> mapTest = h1.getMap(mapName);
        assertEquals("LatestUpdatedValue", mapTest.get("key1"));
        assertEquals("LatestUpdatedValue2", mapTest.get("key2"));
    }

    @Test
    public void testHigherHitsMapMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(HigherHitsMergePolicy.class.getName(), mapName);
        HecatonInstance h1 = Hecaton.newHecatonInstance(config);
        HecatonInstance h2 = Hecaton.newHecatonInstance(config);

        warmUpPartitions(h1, h2);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);

        CountDownLatch mergeBlockingLatch = new CountDownLatch(1);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1, mergeBlockingLatch);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.memberRemovedLatch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        map1.put("key1", "higherHitsValue");
        map1.put("key2", "value2");
        // increase hits number
        map1.get("key1");
        map1.get("key1");

        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put("key1", "value1");
        map2.put("key2", "higherHitsValue2");
        // increase hits number
        map2.get("key2");
        map2.get("key2");

        // allow merge process to continue
        mergeBlockingLatch.countDown();

        assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        assertClusterSizeEventually(2, h1, h2);

        IMap<Object, Object> mapTest = h2.getMap(mapName);
        assertEquals("higherHitsValue", mapTest.get("key1"));
        assertEquals("higherHitsValue2", mapTest.get("key2"));
    }

    @Test
    public void testPutIfAbsentMapMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(PutIfAbsentMergePolicy.class.getName(), mapName);
        HecatonInstance h1 = Hecaton.newHecatonInstance(config);
        HecatonInstance h2 = Hecaton.newHecatonInstance(config);

        warmUpPartitions(h1, h2);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);

        CountDownLatch mergeBlockingLatch = new CountDownLatch(1);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1, mergeBlockingLatch);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.memberRemovedLatch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        map1.put("key1", "PutIfAbsentValue1");

        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put("key1", "value");
        map2.put("key2", "PutIfAbsentValue2");

        // allow merge process to continue
        mergeBlockingLatch.countDown();

        assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        assertClusterSizeEventually(2, h1, h2);

        IMap<Object, Object> mapTest = h2.getMap(mapName);
        assertEquals("PutIfAbsentValue1", mapTest.get("key1"));
        assertEquals("PutIfAbsentValue2", mapTest.get("key2"));
    }

    @Test
    public void testPassThroughMapMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(PassThroughMergePolicy.class.getName(), mapName);
        HecatonInstance h1 = Hecaton.newHecatonInstance(config);
        HecatonInstance h2 = Hecaton.newHecatonInstance(config);

        warmUpPartitions(h1, h2);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);

        CountDownLatch mergeBlockingLatch = new CountDownLatch(1);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1, mergeBlockingLatch);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.memberRemovedLatch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.put(key, "value");

        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put(key, "passThroughValue");

        // allow merge process to continue
        mergeBlockingLatch.countDown();

        assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        assertClusterSizeEventually(2, h1, h2);

        IMap<Object, Object> mapTest = h2.getMap(mapName);
        assertEquals("passThroughValue", mapTest.get(key));
    }

    @Test
    public void testCustomMergePolicy() {
        String mapName = randomMapName();
        Config config = newConfig(TestCustomMapMergePolicy.class.getName(), mapName);
        HecatonInstance h1 = Hecaton.newHecatonInstance(config);
        HecatonInstance h2 = Hecaton.newHecatonInstance(config);

        warmUpPartitions(h1, h2);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(1);
        h2.getCluster().addMembershipListener(memberShipListener);

        CountDownLatch mergeBlockingLatch = new CountDownLatch(1);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1, mergeBlockingLatch);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h2);

        assertOpenEventually(memberShipListener.memberRemovedLatch);
        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        IMap<Object, Object> map1 = h1.getMap(mapName);
        String key = generateKeyOwnedBy(h1);
        map1.put(key, "value");

        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put(key, 1);

        // allow merge process to continue
        mergeBlockingLatch.countDown();

        assertOpenEventually(lifeCycleListener.mergeFinishedLatch);
        assertClusterSizeEventually(2, h1, h2);

        IMap<Object, Object> mapTest = h2.getMap(mapName);
        assertNotNull(mapTest.get(key));
        assertTrue(mapTest.get(key) instanceof Integer);
    }

    private Config newConfig(String mergePolicy, String mapName) {
        Config config = getConfig()
                .setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
                .setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");

        config.setClusterName(generateRandomString(10));

        config.getMapConfig(mapName).setPerEntryStatsEnabled(true)
                .getMergePolicyConfig().setPolicy(mergePolicy);

        return config;
    }

    private class TestLifeCycleListener implements LifecycleListener {

        final CountDownLatch mergeFinishedLatch;
        final CountDownLatch mergeBlockingLatch;

        TestLifeCycleListener(int countdown, CountDownLatch mergeBlockingLatch) {
            this.mergeFinishedLatch = new CountDownLatch(countdown);
            this.mergeBlockingLatch = mergeBlockingLatch;
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGING) {
                try {
                    mergeBlockingLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw rethrow(e);
                }
            } else if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                mergeFinishedLatch.countDown();
            }
        }
    }

    private class TestMemberShipListener implements MembershipListener {

        final CountDownLatch memberRemovedLatch;

        TestMemberShipListener(int countdown) {
            memberRemovedLatch = new CountDownLatch(countdown);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            memberRemovedLatch.countDown();
        }

    }
}
