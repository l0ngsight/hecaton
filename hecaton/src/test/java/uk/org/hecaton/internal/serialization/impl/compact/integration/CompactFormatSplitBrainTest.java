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

package uk.org.hecaton.internal.serialization.impl.compact.integration;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.merge.PutIfAbsentMergePolicy;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.NodeDTO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static uk.org.hecaton.internal.serialization.impl.compact.CompactTestUtil.assertSchemasAvailable;
import static uk.org.hecaton.test.SplitBrainTestSupport.blockCommunicationBetween;
import static uk.org.hecaton.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactFormatSplitBrainTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    private Brain largerBrain;
    private Brain smallerBrain;
    private List<HecatonInstance> allInstances;

    @Before
    public void setUp() {
        Config config = getMemberConfig();

        largerBrain = new Brain(Arrays.asList(
                factory.newHecatonInstance(config),
                factory.newHecatonInstance(config),
                factory.newHecatonInstance(config)
        ));

        smallerBrain = new Brain(Arrays.asList(
                factory.newHecatonInstance(config),
                factory.newHecatonInstance(config)
        ));

        allInstances = new ArrayList<>();
        allInstances.addAll(largerBrain.instances);
        allInstances.addAll(smallerBrain.instances);

        assertClusterSizeEventually(allInstances.size(), allInstances);
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testSplitBrainHealing_whenSmallerClusterHasNoSchemas() {
        splitCluster();

        // Schemas of the Employee and the Node classes are only available in the larger brain
        fillEmployeeMapUsing(largerBrain.getRandomInstance());
        fillNodeMapUsing(largerBrain.getRandomInstance());

        mergeCluster();

        assertEmployeeMapSizeUsing(smallerBrain.getRandomInstance());
        assertNodeMapSizeUsing(smallerBrain.getRandomInstance());

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(smallerBrain.getRandomInstance());
        assertQueryForNodeMapUsing(smallerBrain.getRandomInstance());
    }

    @Test
    public void testSplitBrainHealing_whenLargeClusterHasNoSchemas() {
        splitCluster();

        // Schemas of the Employee and the Node classes are only available in the smaller brain
        fillEmployeeMapUsing(smallerBrain.getRandomInstance());
        fillNodeMapUsing(smallerBrain.getRandomInstance());

        mergeCluster();

        assertEmployeeMapSizeUsing(largerBrain.getRandomInstance());
        assertNodeMapSizeUsing(largerBrain.getRandomInstance());

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(largerBrain.getRandomInstance());
        assertQueryForNodeMapUsing(largerBrain.getRandomInstance());
    }

    @Test
    public void testSplitBrainHealing_whenBothClusterHaveSameSchemas() {
        splitCluster();

        // Schemas of the Employee and the Node classes are only available in both of the brains
        fillEmployeeMapUsing(largerBrain.getRandomInstance());
        fillEmployeeMapUsing(smallerBrain.getRandomInstance());

        fillNodeMapUsing(largerBrain.getRandomInstance());
        fillNodeMapUsing(smallerBrain.getRandomInstance());

        mergeCluster();

        assertEmployeeMapSizeUsing(largerBrain.getRandomInstance());
        assertNodeMapSizeUsing(smallerBrain.getRandomInstance());

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(smallerBrain.getRandomInstance());
        assertQueryForNodeMapUsing(largerBrain.getRandomInstance());
    }

    @Test
    public void testSplitBrainHealing_whenBothClusterHaveDifferentSchemas() {
        splitCluster();

        // The schema of the Employee class is only available in the larger brain
        fillEmployeeMapUsing(largerBrain.getRandomInstance());

        // The schema of the Node class is only available in the smaller brain
        fillNodeMapUsing(smallerBrain.getRandomInstance());

        mergeCluster();

        assertEmployeeMapSizeUsing(smallerBrain.getRandomInstance());
        assertNodeMapSizeUsing(largerBrain.getRandomInstance());

        assertSchemasAvailableInEveryMember();

        assertQueryForEmployeeMapUsing(smallerBrain.getRandomInstance());
        assertQueryForNodeMapUsing(largerBrain.getRandomInstance());
    }

    private static class Brain {
        private final List<HecatonInstance> instances;
        private final Random random = new Random();

        private Brain(List<HecatonInstance> instances) {
            this.instances = instances;
        }

        public HecatonInstance getRandomInstance() {
            return instances.get(random.nextInt(instances.size()));
        }

        public void splitFrom(Brain other) {
            List<HecatonInstance> otherBrainInstances = other.instances;
            for (HecatonInstance instance : instances) {
                for (HecatonInstance otherBrainInstance : otherBrainInstances) {
                    blockCommunicationBetween(instance, otherBrainInstance);
                }
            }

            for (HecatonInstance instance : instances) {
                for (HecatonInstance otherBrainInstance : otherBrainInstances) {
                    closeConnectionBetween(instance, otherBrainInstance);
                }
            }
        }

        public void mergeWith(Brain other) {
            for (HecatonInstance instance : instances) {
                for (HecatonInstance otherBrainInstance : other.instances) {
                    unblockCommunicationBetween(instance, otherBrainInstance);
                }
            }
        }
    }

    private void splitCluster() {
        largerBrain.splitFrom(smallerBrain);

        // make sure that cluster is split
        assertClusterSizeEventually(largerBrain.instances.size(), largerBrain.instances);
        assertClusterSizeEventually(smallerBrain.instances.size(), smallerBrain.instances);
        waitAllForSafeState(allInstances);
    }

    private void mergeCluster() {
        largerBrain.mergeWith(smallerBrain);

        assertClusterSizeEventually(allInstances.size(), allInstances);
        waitAllForSafeState(allInstances);
    }

    private Config getMemberConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getMapConfig("employeeMap")
                .getMergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());
        config.getMapConfig("nodeMap")
                .getMergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "1");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "1");
        return config;
    }

    private void assertSchemasAvailableInEveryMember() {
        assertSchemasAvailable(allInstances, EmployeeDTO.class, NodeDTO.class);
    }

    private void fillEmployeeMapUsing(HecatonInstance instance) {
        IMap<Integer, EmployeeDTO> employeeMap = instance.getMap("employeeMap");
        for (int i = 0; i < 100; i++) {
            EmployeeDTO employeeDTO = new EmployeeDTO(i, i);
            employeeMap.put(i, employeeDTO);
        }
    }

    private void fillNodeMapUsing(HecatonInstance instance) {
        IMap<Integer, NodeDTO> nodeMap = instance.getMap("nodeMap");
        for (int i = 0; i < 100; i++) {
            NodeDTO node = new NodeDTO(new NodeDTO(null, i), i);
            nodeMap.put(i, node);
        }
    }

    private void assertEmployeeMapSizeUsing(HecatonInstance instance) {
        IMap<Integer, EmployeeDTO> employeeMap = instance.getMap("employeeMap");
        assertTrueEventually(() -> assertEquals(100, employeeMap.size()));
    }

    private void assertNodeMapSizeUsing(HecatonInstance instance) {
        IMap<Integer, NodeDTO> nodeMap = instance.getMap("nodeMap");
        assertTrueEventually(() -> assertEquals(100, nodeMap.size()));
    }

    private void assertQueryForEmployeeMapUsing(HecatonInstance instance) {
        IMap<Integer, EmployeeDTO> employeeMap = instance.getMap("employeeMap");
        int size = employeeMap.keySet(Predicates.sql("age > 19")).size();
        assertEquals(80, size);
    }

    private void assertQueryForNodeMapUsing(HecatonInstance instance) {
        IMap<Integer, NodeDTO> nodeMap = instance.getMap("nodeMap");
        int size = nodeMap.keySet(Predicates.sql("child.id > 19")).size();
        assertEquals(80, size);
    }
}
