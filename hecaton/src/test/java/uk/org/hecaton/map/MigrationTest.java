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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.internal.partition.TestPartitionUtils.getAllReplicaAddresses;
import static uk.org.hecaton.internal.partition.TestPartitionUtils.getOngoingReplicaSyncRequests;
import static uk.org.hecaton.internal.partition.TestPartitionUtils.getScheduledReplicaSyncRequests;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MigrationTest extends HecatonTestSupport {

    protected Config getConfig(String mapName) {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB.getName(), "1");
        return config;
    }

    @Test
    public void testMigration_whenAddingInstances_withStatisticsEnabled() {
        int size = 1_000;
        String name = randomString();
        Config config = getConfig(name);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(3);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);

        Map<Integer, Value> map = instance1.getMap(name);
        for (int i = 0; i < size; i++) {
            map.put(i, new Value(i));
        }

        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);
        waitAllForSafeState(instance1, instance2);

        assertEquals("Some records have been lost.", size, map.values().size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i).value);
        }

        HecatonInstance instance3 = nodeFactory.newHecatonInstance(config);
        waitAllForSafeState(instance1, instance2, instance3);

        assertEquals("Some records have been lost.", size, map.values().size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i).value);
        }

        List<HecatonInstance> list = new ArrayList<>(3);
        list.add(instance1);
        list.add(instance2);
        list.add(instance3);
        assertThatMigrationIsDoneAndReplicasAreIntact(list);
    }

    @Test
    public void testMigration_whenRemovingInstances_withStatisticsDisabled() {
        int size = 1_000;
        String name = randomString();
        Config config = getConfig(name);
        MapConfig mapConfig = config.getMapConfig(name);
        mapConfig.setStatisticsEnabled(false);

        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(3);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance3 = nodeFactory.newHecatonInstance(config);

        IMap<Integer, Value> map = instance1.getMap(name);
        for (int i = 0; i < size; i++) {
            map.put(i, new Value(i));
        }
        instance2.shutdown();
        instance3.shutdown();

        waitAllForSafeState(instance1);
        assertEquals("Some records have been lost.", size, map.values().size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i).value);
        }
        assertThatMigrationIsDoneAndReplicasAreIntact(singletonList(instance1));
    }

    private static void assertThatMigrationIsDoneAndReplicasAreIntact(List<HecatonInstance> list) {
        // assert that we have as much replicas as instances in the given list
        for (Map.Entry<Integer, List<Address>> entry : getAllReplicaAddresses(list).entrySet()) {
            int replicaIndex = 0;
            for (Address address : entry.getValue()) {
                assertNotNull("Replica of index " + replicaIndex + " is lost!", address);
                if (++replicaIndex == list.size()) {
                    break;
                }
            }
        }

        // assert that no replica syncs are ongoing or scheduled (migrations should be done)
        for (HecatonInstance instance : list) {
            assertEquals(0, getOngoingReplicaSyncRequests(instance).size());
            assertEquals(0, getScheduledReplicaSyncRequests(instance).size());
        }
    }

    static class Value implements Serializable {

        int value;
        @SuppressWarnings("unused")
        byte[] payload = new byte[100 * 1024];

        Value(int value) {
            this.value = value;
        }

    }

}
