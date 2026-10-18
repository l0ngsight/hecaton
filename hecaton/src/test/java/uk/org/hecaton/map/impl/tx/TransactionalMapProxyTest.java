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

package uk.org.hecaton.map.impl.tx;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapStoreConfig.InitialLoadMode;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.MapStoreConfig.InitialLoadMode.EAGER;
import static uk.org.hecaton.config.MapStoreConfig.InitialLoadMode.LAZY;
import static org.junit.Assert.assertEquals;

/**
 * Tests the creation of a transactional map proxy.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionalMapProxyTest extends HecatonTestSupport {

    private final String mapName = randomMapName();

    @Test
    public void whenMapProxyIsCreated_mapContainerIsNotCreated() {
        Config config = new Config();

        TransactionalMapProxy mapProxy = getTransactionalMapProxy(config);
        assertNoMapContainersExist(mapProxy);
    }

    @Test
    public void whenMapProxyWithLazyMapStoreIsCreated_mapContainerIsNotCreated() {
        Config config = getConfigWithMapStore(LAZY);

        TransactionalMapProxy mapProxy = getTransactionalMapProxy(config);
        assertNoMapContainersExist(mapProxy);
    }

    @Test
    @Ignore(value = "PartitionContainer#createRecordStore() creates a MapContainer, which we cannot avoid for now")
    public void whenMapProxyWithEagerMapStoreIsCreated_mapContainerIsNotCreated() {
        Config config = getConfigWithMapStore(EAGER);

        TransactionalMapProxy mapProxy = getTransactionalMapProxy(config);
        assertNoMapContainersExist(mapProxy);
    }

    @Test
    public void whenNearCachedMapProxyIsCreated_mapContainerIsNotCreated() {
        Config config = new Config();
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(mapName)
                .setInMemoryFormat(BINARY)
                .setInvalidateOnChange(false);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        TransactionalMapProxy mapProxy = getTransactionalMapProxy(config);
        assertNoMapContainersExist(mapProxy);
    }

    private Config getConfigWithMapStore(InitialLoadMode loadMode) {
        Config config = new Config();
        config.getMapConfig(mapName)
                .getMapStoreConfig().setClassName("uk.org.hecaton.config.helpers.DummyMapStore")
                .setInitialLoadMode(loadMode)
                .setEnabled(true);
        return config;
    }

    private TransactionalMapProxy getTransactionalMapProxy(Config config) {
        HecatonInstance hz = createHecatonInstance(config);
        TransactionContext transactionContext = hz.newTransactionContext();
        transactionContext.beginTransaction();
        return (TransactionalMapProxy) transactionContext.getMap(mapName);
    }

    private void assertNoMapContainersExist(TransactionalMapProxy map) {
        MapServiceContext mapServiceContext = map.getService().getMapServiceContext();
        assertEquals(0, mapServiceContext.getMapContainers().size());
    }
}
