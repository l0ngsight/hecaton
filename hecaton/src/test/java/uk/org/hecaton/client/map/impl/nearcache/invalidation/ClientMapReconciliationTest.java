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

package uk.org.hecaton.client.map.impl.nearcache.invalidation;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.map.impl.nearcache.NearCachedClientMapProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.MIN_RECONCILIATION_INTERVAL_SECONDS;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static uk.org.hecaton.map.impl.nearcache.invalidation.MemberMapReconciliationTest.assertStats;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapReconciliationTest extends HecatonTestSupport {

    private static final String MAP_1_NAME = "ClientMapReconciliationTest-map-1";
    private static final String MAP_2_NAME = "ClientMapReconciliationTest-map-2";
    private static final String MAP_3_NAME = "ClientMapReconciliationTest-map-3";

    private static final int RECONCILIATION_INTERVAL_SECS = 3;

    private final TestHecatonFactory factory = new TestHecatonFactory();

    private ClientConfig clientConfig;

    // servers
    private IMap<Integer, Integer> serverMap1;
    private IMap<Integer, Integer> serverMap2;
    private IMap<Integer, Integer> serverMap3;
    // clients
    private IMap<Integer, Integer> clientMap1;
    private IMap<Integer, Integer> clientMap2;
    private IMap<Integer, Integer> clientMap3;

    @Before
    public void setUp() {
        Config config = getBaseConfig()
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE))
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(Integer.MAX_VALUE));

        NearCacheConfig nearCacheConfig = new NearCacheConfig("*")
                .setInvalidateOnChange(true);

        clientConfig = new ClientConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), String.valueOf(RECONCILIATION_INTERVAL_SECS))
                .setProperty(MIN_RECONCILIATION_INTERVAL_SECONDS.getName(), String.valueOf(RECONCILIATION_INTERVAL_SECS))
                .addNearCacheConfig(nearCacheConfig);

        HecatonInstance server = factory.newHecatonInstance(config);
        serverMap1 = server.getMap(MAP_1_NAME);
        serverMap2 = server.getMap(MAP_2_NAME);
        serverMap3 = server.getMap(MAP_3_NAME);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void test_reconciliation_does_not_cause_premature_removal() {
        int total = 100;
        for (int i = 0; i < total; i++) {
            serverMap1.put(i, i);
        }

        clientMap1 = createMapFromNewClient(MAP_1_NAME);

        for (int i = 0; i < total; i++) {
            clientMap1.get(i);
        }

        IMap mapFromNewClient = createMapFromNewClient(MAP_1_NAME);
        for (int i = 0; i < total; i++) {
            mapFromNewClient.get(i);
        }

        NearCacheStats nearCacheStats = mapFromNewClient.getLocalMapStats().getNearCacheStats();
        assertStats(MAP_1_NAME, nearCacheStats, total, 0, total);

        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECS);

        for (int i = 0; i < total; i++) {
            mapFromNewClient.get(i);
        }

        assertStats(MAP_1_NAME, nearCacheStats, total, total, total);
    }

    @Test
    public void test_reconciliation_does_not_cause_premature_removal_on_other_maps_after_map_clear() {
        int total = 91;
        for (int i = 0; i < total; i++) {
            serverMap1.put(i, i);
            serverMap2.put(i, i);
            serverMap3.put(i, i);
        }

        clientMap1 = createMapFromNewClient(MAP_1_NAME);
        clientMap2 = createMapFromNewClient(MAP_2_NAME);
        clientMap3 = createMapFromNewClient(MAP_3_NAME);

        for (int i = 0; i < total; i++) {
            clientMap1.get(i);
            clientMap2.get(i);
            clientMap3.get(i);
        }

        assertStats(MAP_1_NAME, clientMap1.getLocalMapStats().getNearCacheStats(), total, 0, total);
        assertStats(MAP_2_NAME, clientMap2.getLocalMapStats().getNearCacheStats(), total, 0, total);
        assertStats(MAP_3_NAME, clientMap3.getLocalMapStats().getNearCacheStats(), total, 0, total);

        // Call map.clear on 1st map
        clientMap1.clear();

        // Sleep a little, hence we can see effect of reconciliation-task.
        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECS);

        // Do subsequent gets on maps.
        // Except for 1st map, other maps should
        // return responses from their near-cache.
        for (int i = 0; i < total; i++) {
            clientMap1.get(i);
            clientMap2.get(i);
            clientMap3.get(i);
        }

        // clientMap1 caches null values so, at
        // here, it's near cache should not be empty
        // despite we cleared its backing map before.
        assertStats(MAP_1_NAME, clientMap1.getLocalMapStats().getNearCacheStats(), total, 0, 2 * total);
        assertStats(MAP_2_NAME, clientMap2.getLocalMapStats().getNearCacheStats(), total, total, total);
        assertStats(MAP_3_NAME, clientMap3.getLocalMapStats().getNearCacheStats(), total, total, total);
    }

    private IMap<Integer, Integer> createMapFromNewClient(String mapName) {
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap(mapName);

        assertInstanceOf(NearCachedClientMapProxy.class, map);

        return map;
    }
}
