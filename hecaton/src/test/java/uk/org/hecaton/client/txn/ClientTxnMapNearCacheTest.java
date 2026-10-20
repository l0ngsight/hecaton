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

package uk.org.hecaton.client.txn;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionalMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnMapNearCacheTest extends HecatonTestSupport {

    @Parameterized.Parameter
    public boolean serializeKeys;

    @Parameterized.Parameters(name = "serializeKeys:{0},")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    private static final String MAP_NAME = "default";
    private static final int KEY_COUNT = 1_000;

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance server;
    private HecatonInstance client;
    private IMap<Integer, Integer> serverMap;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        Config config = smallInstanceConfig();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setSerializeKeys(serializeKeys);
        nearCacheConfig.setCacheLocalEntries(true);

        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        server = hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        client = hecatonFactory.newHecatonClient(clientConfig);

        serverMap = server.getMap(MAP_NAME);

        // populate server-side-map
        for (int i = 0; i < KEY_COUNT; i++) {
            serverMap.set(i, i);
        }

        // populate server-side-map's near-cache
        for (int i = 0; i < KEY_COUNT; i++) {
            serverMap.get(i);
        }
    }

    @Test
    public void put_invalidates_server_side_near_cache() {
        // update map in a client txn
        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            txnMap.put(i, i);
        }
        context.commitTransaction();

        NearCacheStats nearCacheStats = serverMap.getLocalMapStats().getNearCacheStats();
        long ownedEntryCount = nearCacheStats.getOwnedEntryCount();
        long invalidations = nearCacheStats.getInvalidations();

        assertEquals(0, ownedEntryCount);
        assertEquals(KEY_COUNT, invalidations);
    }

    @Test
    public void remove_invalidates_server_side_near_cache() {
        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            txnMap.remove(i);
        }
        context.commitTransaction();

        NearCacheStats nearCacheStats = serverMap.getLocalMapStats().getNearCacheStats();
        long ownedEntryCount = nearCacheStats.getOwnedEntryCount();
        long invalidations = nearCacheStats.getInvalidations();

        assertEquals(0, ownedEntryCount);
        assertEquals(KEY_COUNT, invalidations);
    }

    @Test
    public void removeIfSame_invalidates_server_side_near_cache() {
        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            txnMap.remove(i, i);
        }
        context.commitTransaction();

        NearCacheStats nearCacheStats = serverMap.getLocalMapStats().getNearCacheStats();
        long ownedEntryCount = nearCacheStats.getOwnedEntryCount();
        long invalidations = nearCacheStats.getInvalidations();

        assertEquals(0, ownedEntryCount);
        assertEquals(KEY_COUNT, invalidations);
    }

    @Test
    public void replace_invalidates_server_side_near_cache() {
        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            txnMap.replace(i, i);
        }
        context.commitTransaction();

        NearCacheStats nearCacheStats = serverMap.getLocalMapStats().getNearCacheStats();
        long ownedEntryCount = nearCacheStats.getOwnedEntryCount();
        long invalidations = nearCacheStats.getInvalidations();

        assertEquals(0, ownedEntryCount);
        assertEquals(KEY_COUNT, invalidations);
    }

    @Test
    public void replaceIfSame_invalidates_server_side_near_cache() {
        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> txnMap = context.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            txnMap.replace(i, i, i);
        }
        context.commitTransaction();

        NearCacheStats nearCacheStats = serverMap.getLocalMapStats().getNearCacheStats();
        long ownedEntryCount = nearCacheStats.getOwnedEntryCount();
        long invalidations = nearCacheStats.getInvalidations();

        assertEquals(0, ownedEntryCount);
        assertEquals(KEY_COUNT, invalidations);
    }
}
