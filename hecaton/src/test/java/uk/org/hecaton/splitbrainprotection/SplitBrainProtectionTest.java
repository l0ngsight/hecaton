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

package uk.org.hecaton.splitbrainprotection;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.SplitBrainProtectionConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.splitbrainprotection.impl.ProbabilisticSplitBrainProtectionFunction;
import uk.org.hecaton.splitbrainprotection.impl.RecentlyActiveSplitBrainProtectionFunction;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests split brain protection related configurations.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SplitBrainProtectionTest extends HecatonTestSupport {

    @Test
    public void testSplitBrainProtectionNotChecked_whenIsNotEnabled() {
        String disabledSBPName = "disabled-quorum";
        SplitBrainProtectionConfig disabledSBPConfig = new SplitBrainProtectionConfig()
                .setName(disabledSBPName)
                .setMinimumClusterSize(3);

        MapConfig mapConfig = new MapConfig(randomMapName())
                .setSplitBrainProtectionName(disabledSBPName);

        Config config = new Config()
                .addSplitBrainProtectionConfig(disabledSBPConfig)
                .addMapConfig(mapConfig);
        HecatonInstance instance = createHecatonInstance(config);

        instance.getSplitBrainProtectionService().ensureNoSplitBrain(disabledSBPName, disabledSBPConfig.getProtectOn());
        instance.getMap(mapConfig.getName()).put("key", "value");
    }

    @Test
    public void testSplitBrainProtectionIsSetCorrectlyOnNodeInitialization() {
        String splitBrainProtectionName1 = randomString();
        String splitBrainProtectionName2 = randomString();

        SplitBrainProtectionConfig splitBrainProtectionConfig1 = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName1)
                .setEnabled(true)
                .setFunctionImplementation(members -> true);

        SplitBrainProtectionConfig splitBrainProtectionConfig2 = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName2)
                .setEnabled(true)
                .setMinimumClusterSize(2);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig1)
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig2);

        HecatonInstance hecatonInstance = createHecatonInstance(config);
        final SplitBrainProtection splitBrainProtection1 = hecatonInstance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName1);
        final SplitBrainProtection splitBrainProtection2 = hecatonInstance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName2);
        assertTrueEventually(() -> {
            assertTrue(splitBrainProtection1.hasMinimumSize());
            assertFalse(splitBrainProtection2.hasMinimumSize());
        });
    }

    @Test
    public void testProbabilisticSplitBrainProtectionConsidersLocalMember() {
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionFunction splitBrainProtectionFunction = new ProbabilisticSplitBrainProtectionFunction(1, 100, 1250, 20, 100, 20);
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(splitBrainProtectionFunction);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig)
                .setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");

        HecatonInstance instance = createHecatonInstance(config);

        final SplitBrainProtection splitBrainProtection = instance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName);

        assertTrueEventually(() -> assertTrue(splitBrainProtection.hasMinimumSize()));
    }

    @Test
    public void testRecentlyActiveSplitBrainProtectionConsidersLocalMember() {
        final String splitBrainProtectionName = randomString();
        SplitBrainProtectionFunction splitBrainProtectionFunction = new RecentlyActiveSplitBrainProtectionFunction(1, 10000);
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(splitBrainProtectionFunction);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig)
                .setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");

        HecatonInstance instance = createHecatonInstance(config);

        final SplitBrainProtection splitBrainProtection = instance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName);

        assertTrueEventually(() -> assertTrue(splitBrainProtection.hasMinimumSize()));
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void testCustomSplitBrainProtectionFunctionFails() {
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();

        MapConfig mapConfig = new MapConfig(mapName)
                .setSplitBrainProtectionName(splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(members -> false);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig)
                .addMapConfig(mapConfig);

        HecatonInstance hecatonInstance = createHecatonInstance(config);
        IMap<Object, Object> map = hecatonInstance.getMap(mapName);
        map.put("1", "1");
    }

    @Test
    public void testCustomSplitBrainProtectionFunctionIsPresent() {
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();

        MapConfig mapConfig = new MapConfig(mapName)
                .setSplitBrainProtectionName(splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(members -> false);

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig)
                .addMapConfig(mapConfig);

        HecatonInstance hecatonInstance = createHecatonInstance(config);
        IMap<Object, Object> map = hecatonInstance.getMap(mapName);
        try {
            map.put("1", "1");
            fail();
        } catch (Exception ignored) {
        }
        SplitBrainProtection splitBrainProtection = hecatonInstance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionName);
        assertFalse(splitBrainProtection.hasMinimumSize());
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void testCustomSplitBrainProtectionFunctionFailsForAllNodes() {
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();

        MapConfig mapConfig = new MapConfig(mapName)
                .setSplitBrainProtectionName(splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(members -> false);

        Config config = new Config()
                .addMapConfig(mapConfig)
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        factory.newHecatonInstance(config);
        HecatonInstance hz = factory.newHecatonInstance(config);
        IMap<Object, Object> map2 = hz.getMap(mapName);
        map2.put("1", "1");
    }

    @Test
    public void testCustomSplitBrainProtectionFunctionFailsThenSuccess() {
        String mapName = randomMapName();
        String splitBrainProtectionName = randomString();

        MapConfig mapConfig = new MapConfig(mapName)
                .setSplitBrainProtectionName(splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .setFunctionImplementation(members -> members.size() >= 2);

        Config config = new Config()
                .addMapConfig(mapConfig)
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hecatonInstance = factory.newHecatonInstance(config);
        IMap<Object, Object> map = hecatonInstance.getMap(mapName);
        try {
            map.put("1", "1");
            fail();
        } catch (SplitBrainProtectionException ignored) {
        }
        factory.newHecatonInstance(config);
        map.put("1", "1");
    }

    @Test
    public void testOneSplitBrainProtectionsFailsOneSplitBrainProtectionSuccessForDifferentMaps() {
        String fourNodeSplitBrainProtection = randomString();
        String threeNodeSplitBrainProtection = randomString();

        MapConfig fourNodeMapConfig = new MapConfig("fourNode")
                .setSplitBrainProtectionName(fourNodeSplitBrainProtection);
        MapConfig threeNodeMapConfig = new MapConfig("threeNode")
                .setSplitBrainProtectionName(threeNodeSplitBrainProtection);

        SplitBrainProtectionConfig fourNodeSplitBrainProtectionConfig = new SplitBrainProtectionConfig(fourNodeSplitBrainProtection, true)
                .setFunctionImplementation(members -> members.size() == 4);
        SplitBrainProtectionConfig threeNodeSplitBrainProtectionConfig = new SplitBrainProtectionConfig(threeNodeSplitBrainProtection, true)
                .setFunctionImplementation(members -> members.size() == 3);

        Config config = new Config()
                .addMapConfig(fourNodeMapConfig)
                .addMapConfig(threeNodeMapConfig)
                .addSplitBrainProtectionConfig(threeNodeSplitBrainProtectionConfig)
                .addSplitBrainProtectionConfig(fourNodeSplitBrainProtectionConfig);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance hz = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        IMap<Object, Object> fourNode = hz.getMap("fourNode");
        IMap<Object, Object> threeNode = hz.getMap("threeNode");
        threeNode.put(generateKeyOwnedBy(hz), "bar");
        try {
            fourNode.put(generateKeyOwnedBy(hz), "bar");
            fail();
        } catch (Exception ignored) {
        }
    }

    /**
     * https://github.com/hazelcast/hazelcast/issues/9792
     */
    @Test
    public void oneSplitBrainProtectionShouldNotAffectSplitBrainProtectionAwareOperationsOnDataStructuresWithoutSplitBrainProtectionConfiguration() {
        String splitBrainProtectionName = randomString();

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true)
                .setFunctionImplementation(members -> members.size() == 3);

        MapConfig mapConfig = new MapConfig("splitBrainProtectionMap")
                .setSplitBrainProtectionName(splitBrainProtectionName);

        Config config = new Config()
                .addMapConfig(mapConfig)
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance hz = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);

        IMap<Object, Object> splitBrainProtectionMap = hz.getMap("splitBrainProtectionMap");
        splitBrainProtectionMap.put(generateKeyOwnedBy(hz), "bar");

        IQueue<String> queue = hz.getQueue("noSplitBrainProtectionQueue");
        queue.offer("item");
    }

    @Test
    public void givenSplitBrainProtectionFunctionConfigured_whenImplementsHecatonInstanceAware_thenHecatonInjectsItsInstance() {
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(randomString(), true)
                .setFunctionClassName(HecatonInstanceAwareSplitBrainProtectionFunction.class.getName());

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        HecatonInstance instance = createHecatonInstance(config);
        assertEquals(instance, HecatonInstanceAwareSplitBrainProtectionFunction.instance);
    }

    @Test
    public void givenSplitBrainProtectionFunctionInstanceConfigured_whenImplementsHecatonInstanceAware_thenHecatonInjectsItsInstance() {
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(randomString(), true)
                .setFunctionImplementation(new HecatonInstanceAwareSplitBrainProtectionFunction());

        Config config = new Config()
                .addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        HecatonInstance instance = createHecatonInstance(config);
        assertEquals(instance, HecatonInstanceAwareSplitBrainProtectionFunction.instance);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void givenProbabilisticSplitBrainProtection_whenAcceptableHeartbeatPause_greaterThanMaxNoHeartbeat_exceptionIsThrown() {
        Config config = new Config();
        config.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
        SplitBrainProtectionConfig probabilisticSplitBrainProtectionConfig = SplitBrainProtectionConfig.newProbabilisticSplitBrainProtectionConfigBuilder("prob-split-brain-protection", 3)
                .withAcceptableHeartbeatPauseMillis(13000)
                .build();

        config.addSplitBrainProtectionConfig(probabilisticSplitBrainProtectionConfig);

        createHecatonInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void givenProbabilisticSplitBrainProtection_whenAcceptableHeartbeatPause_lessThanHeartbeatInterval_exceptionIsThrown() {
        Config config = new Config();
        config.setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "5");
        SplitBrainProtectionConfig probabilisticSplitBrainProtectionConfig = SplitBrainProtectionConfig.newProbabilisticSplitBrainProtectionConfigBuilder("prob-split-brain-protection", 3)
                .withAcceptableHeartbeatPauseMillis(3000)
                .build();

        config.addSplitBrainProtectionConfig(probabilisticSplitBrainProtectionConfig);

        createHecatonInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void givenRecentlyActiveSplitBrainProtection_whenHeartbeatTolerance_greaterThanMaxNoHeartbeat_exceptionIsThrown() {
        Config config = new Config();
        config.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
        SplitBrainProtectionConfig recentlyActiveSplitBrainProtectionConfig = SplitBrainProtectionConfig
                .newRecentlyActiveSplitBrainProtectionConfigBuilder("test-splitBrainProtection", 3, 13000)
                .build();

        config.addSplitBrainProtectionConfig(recentlyActiveSplitBrainProtectionConfig);

        createHecatonInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void givenRecentlyActiveSplitBrainProtection_whenHeartbeatTolerance_lessThanHeartbeatInterval_exceptionIsThrown() {
        Config config = new Config();
        config.setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "5");
        SplitBrainProtectionConfig recentlyActiveSplitBrainProtectionConfig = SplitBrainProtectionConfig
                .newRecentlyActiveSplitBrainProtectionConfigBuilder("test-splitBrainProtection", 3, 3000)
                .build();

        config.addSplitBrainProtectionConfig(recentlyActiveSplitBrainProtectionConfig);

        createHecatonInstance(config);
    }

    private static class HecatonInstanceAwareSplitBrainProtectionFunction implements SplitBrainProtectionFunction, HecatonInstanceAware {

        private static volatile HecatonInstance instance;

        @Override
        public void setHecatonInstance(HecatonInstance instance) {
            HecatonInstanceAwareSplitBrainProtectionFunction.instance = instance;
        }

        @Override
        public boolean apply(Collection<Member> members) {
            return false;
        }
    }
}
