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

package uk.org.hecaton.replicatedmap;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.spi.merge.HigherHitsMergePolicy;
import uk.org.hecaton.spi.merge.LatestUpdateMergePolicy;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.PutIfAbsentMergePolicy;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.test.SplitBrainTestSupport.blockCommunicationBetween;
import static uk.org.hecaton.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class ReplicatedMapMergePolicyTest extends HecatonTestSupport {

    @Parameters(name = "{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{
                new LatestUpdateMergePolicyTestCase(),
                new HighestHitsMergePolicyTestCase(),
                new PutIfAbsentMapMergePolicyTestCase(),
                new PassThroughMapMergePolicyTestCase(),
                new CustomMergePolicyTestCase(),
        });
    }

    @Parameter
    public ReplicatedMapMergePolicyTestCase testCase;

    private TestHecatonInstanceFactory factory;

    @Before
    public void init() {
        factory = createHecatonInstanceFactory(2);
    }

    @Test
    public void testMapMergePolicy() {
        final String mapName = randomMapName();
        Config config = newConfig(testCase.getMergePolicyClassName(), mapName);
        final HecatonInstance h1 = factory.newHecatonInstance(config);
        final HecatonInstance h2 = factory.newHecatonInstance(config);

        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h2.getLifecycleService().addLifecycleListener(lifeCycleListener);

        // wait for cluster to be formed before breaking the connection
        waitAllForSafeState(h1, h2);

        blockCommunicationBetween(h1, h2);
        closeConnectionBetween(h1, h2);

        assertClusterSizeEventually(1, h1);
        assertClusterSizeEventually(1, h2);

        ReplicatedMap<Object, Object> map1 = h1.getReplicatedMap(mapName);
        ReplicatedMap<Object, Object> map2 = h2.getReplicatedMap(mapName);
        final Map<Object, Object> expectedValues = testCase.populateMaps(map1, map2, h1);

        unblockCommunicationBetween(h1, h2);

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(2, h1, h2);

        assertTrueEventually(() -> {
            ReplicatedMap<Object, Object> mapTest = h1.getReplicatedMap(mapName);
            for (Map.Entry<Object, Object> entry : expectedValues.entrySet()) {
                assertEquals(entry.getValue(), mapTest.get(entry.getKey()));
            }
        });
    }

    private Config newConfig(String mergePolicy, String mapName) {
        Config config = new Config();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        config.setClusterName(generateRandomString(10));
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(mapName);
        replicatedMapConfig.getMergePolicyConfig().setPolicy(mergePolicy);
        return config;
    }

    private class TestLifeCycleListener implements LifecycleListener {

        CountDownLatch latch;

        TestLifeCycleListener(int countdown) {
            latch = new CountDownLatch(countdown);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }
    }

    private interface ReplicatedMapMergePolicyTestCase {
        // Populate given maps with K-V pairs. Optional HZ instance is required by specific merge policies in order to
        // generate keys owned by the given instance.
        // return K-V pairs expected to be found in the merged map
        Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                         HecatonInstance instance);

        // return merge policy's class name
        String getMergePolicyClassName();
    }

    private static class LatestUpdateMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HecatonInstance instance) {
            map1.put("key1", "value");
            // prevent updating at the same time
            sleepAtLeastSeconds(1);
            map2.put("key1", "LatestUpdatedValue");
            map2.put("key2", "value2");
            // prevent updating at the same time
            sleepAtLeastSeconds(1);
            map1.put("key2", "LatestUpdatedValue2");

            Map<Object, Object> expectedValues = new HashMap<>();
            expectedValues.put("key1", "LatestUpdatedValue");
            expectedValues.put("key2", "LatestUpdatedValue2");
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return LatestUpdateMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "LatestUpdateMapMergePolicy";
        }
    }

    private static class HighestHitsMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HecatonInstance instance) {
            map1.put("key1", "higherHitsValue");
            map1.put("key2", "value2");
            // increase hits number
            map1.get("key1");
            map1.get("key1");

            map2.put("key1", "value1");
            map2.put("key2", "higherHitsValue2");
            // increase hits number
            map2.get("key2");
            map2.get("key2");

            Map<Object, Object> expectedValues = new HashMap<>();
            expectedValues.put("key1", "higherHitsValue");
            expectedValues.put("key2", "higherHitsValue2");
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return HigherHitsMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "HigherHitsMapMergePolicy";
        }
    }

    private static class PutIfAbsentMapMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HecatonInstance instance) {
            map1.put("key1", "PutIfAbsentValue1");

            map2.put("key1", "value");
            map2.put("key2", "PutIfAbsentValue2");

            Map<Object, Object> expectedValues = new HashMap<>();
            expectedValues.put("key1", "PutIfAbsentValue1");
            expectedValues.put("key2", "PutIfAbsentValue2");
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return PutIfAbsentMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "PutIfAbsentMapMergePolicy";
        }
    }

    private static class PassThroughMapMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HecatonInstance instance) {
            assertNotNull(instance);
            String key = generateKeyOwnedBy(instance);
            map1.put(key, "value");

            map2.put(key, "passThroughValue");
            Map<Object, Object> expectedValues = new HashMap<>();
            expectedValues.put(key, "passThroughValue");
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return PassThroughMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "PassThroughMergePolicy";
        }
    }

    private static class CustomMergePolicyTestCase implements ReplicatedMapMergePolicyTestCase {

        @Override
        public Map<Object, Object> populateMaps(ReplicatedMap<Object, Object> map1, ReplicatedMap<Object, Object> map2,
                                                HecatonInstance instance) {
            assertNotNull(instance);
            String key = generateKeyOwnedBy(instance);
            Integer value = 1;
            map1.put(key, "value");

            map2.put(key, value);

            Map<Object, Object> expectedValues = new HashMap<>();
            expectedValues.put(key, value);
            return expectedValues;
        }

        @Override
        public String getMergePolicyClassName() {
            return CustomReplicatedMapMergePolicy.class.getName();
        }

        @Override
        public String toString() {
            return "CustomMergePolicy";
        }
    }
}
