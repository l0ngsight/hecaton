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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapLiteMemberTest extends HecatonTestSupport {

    private Config dataMemberConfig = buildConfig(false);
    private Config liteMemberConfig = buildConfig(true);

    @Test
    public void testLiteMembersWithReplicatedMap() {
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(3);

        final HecatonInstance instance1 = nodeFactory.newHecatonInstance(dataMemberConfig);
        final HecatonInstance instance2 = nodeFactory.newHecatonInstance(dataMemberConfig);
        final HecatonInstance lite = nodeFactory.newHecatonInstance(liteMemberConfig);

        ReplicatedMap<String, String> replicatedMap = instance1.getReplicatedMap("default");

        replicatedMap.put("key", "value");

        assertTrueEventually(() -> {
            assertTrue(instance1.getReplicatedMap("default").containsKey("key"));
            assertTrue(instance2.getReplicatedMap("default").containsKey("key"));
        });
        assertTrueAllTheTime(() -> {
            ReplicatedMapService service = getReplicatedMapService(lite);
            assertEquals(0, service.getAllReplicatedRecordStores("default").size());
        }, 3);
    }

    @Test
    public void testPromoteLiteMember() {
        String mapName = randomName();
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(dataMemberConfig);

        ReplicatedMap<String, String> map = instance1.getReplicatedMap(mapName);
        map.put("key", "value");

        HecatonInstance instance2 = nodeFactory.newHecatonInstance(liteMemberConfig);
        instance2.getCluster().promoteLocalLiteMember();

        final ReplicatedMap<String, String> promotedMap = instance2.getReplicatedMap(mapName);
        assertTrueEventually(() -> assertEquals("Expected the promoted lite member to retrieve a value from a ReplicatedMap",
                "value", promotedMap.get("key")));
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testCreateReplicatedMapOnLiteMember() {
        HecatonInstance lite = createSingleLiteMember();
        lite.getReplicatedMap("default");
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testCreateReplicatedStoreOnLiteMember() {
        HecatonInstance lite = createSingleLiteMember();
        ReplicatedMapService service = getReplicatedMapService(lite);
        service.getReplicatedRecordStore("default", true, 1);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testGetReplicatedStoreOnLiteMember() {
        HecatonInstance lite = createSingleLiteMember();
        ReplicatedMapService service = getReplicatedMapService(lite);
        service.getReplicatedRecordStore("default", false, 1);
    }

    private HecatonInstance createSingleLiteMember() {
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(1);
        return nodeFactory.newHecatonInstance(liteMemberConfig);
    }

    private ReplicatedMapService getReplicatedMapService(HecatonInstance instance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        return nodeEngine.getService(SERVICE_NAME);
    }

    private Config buildConfig(boolean liteMember) {
        return smallInstanceConfig()
                .setLiteMember(liteMember);
    }
}
