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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapConfigurationPassiveTest extends HecatonTestSupport {

    private final int clusterSize = 1;
    private TestHecatonFactory hecatonFactory;
    private HecatonInstance[] instances;
    private HecatonInstance client;

    @Before
    public void setup() {
        hecatonFactory = new TestHecatonFactory(clusterSize);
        instances = hecatonFactory.newInstances();
        client = hecatonFactory.newHecatonClient();
    }

    @After
    public void stopHecatonInstances() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void whenClusterInPassiveState_thenIndexCannotBeAdded_fromMember() {
        whenClusterInPassiveState_thenIndexCannotBeAdded(instances[0]);
    }

    @Test
    public void whenClusterInPassiveState_thenIndexCannotBeAdded_fromClient() {
        whenClusterInPassiveState_thenIndexCannotBeAdded(client);
    }

    private void whenClusterInPassiveState_thenIndexCannotBeAdded(HecatonInstance hz) {
        String mapWithIndexName = randomMapName();
        MapConfig mapWithIndex = new MapConfig(mapWithIndexName)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "this"));
        hz.getConfig().addMapConfig(mapWithIndex);
        var map = hz.getMap(mapWithIndexName);

        waitAllForSafeState(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        assertThatThrownBy(() -> map.addIndex(IndexType.SORTED, "this"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot add index when cluster is in PASSIVE state!");

        hz.getMap(mapWithIndexName).get(1);
    }

    @Test
    public void whenClusterInPassiveState_thenIMapWithIndexCanBeCreated_fromMember() {
        HecatonInstance hz = instances[0];
        String mapName = randomMapName();

        MapConfig mapWithIndex = new MapConfig(mapName)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "this").setName("index"));
        hz.getConfig().addMapConfig(mapWithIndex);

        warmUpPartitions(instances);
        waitAllForSafeState(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        // Index is created when MapProxy is initialized. This will happen also for read only
        // operations on maps that have not yet been created.
        IMap<Object, Object> map = hz.getMap(mapName);
        assertThat(map.get(1)).as("Should not throw").isNull();
    }

    @Test
    public void whenClusterInPassiveState_thenIMapWithIndexCannotBeCreated_fromClient() {
        String mapName = randomMapName();

        MapConfig mapWithIndex = new MapConfig(mapName)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "this").setName("index"));
        client.getConfig().addMapConfig(mapWithIndex);

        warmUpPartitions(instances);
        waitAllForSafeState(instances);
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        // Unlike member api, getMap from client invokes InitializeDistributedObjectOperation
        // which is not allowed in PASSIVE state
        assertThatThrownBy(() -> client.getMap(mapName))
                .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Cluster is in PASSIVE state!");
    }
}
