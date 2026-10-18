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
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.adapter.TransactionalMapDataStructureAdapter;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.AbstractNearCacheLeakTest;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContext;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContextBuilder;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.nearcache.MapNearCacheManager;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionalMap;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getMapNearCacheManager;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;

/**
 * Basic Near Cache tests for {@link TransactionalMap} on Hecaton members.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TxnMapNearCacheLeakTest extends AbstractNearCacheLeakTest<Data, String> {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean serializeKeys;

    private final TestHecatonInstanceFactory hecatonFactory = createHecatonInstanceFactory(2);

    @Parameters(name = "format:{0} serializeKeys:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(inMemoryFormat, serializeKeys)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig(false);

        HecatonInstance dataInstance = hecatonFactory.newHecatonInstance(config);
        TransactionalMapDataStructureAdapter<K, V> dataAdapter
                = new TransactionalMapDataStructureAdapter<>(dataInstance, DEFAULT_NEAR_CACHE_NAME);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(dataInstance)
                .setDataAdapter(dataAdapter)
                .build();
    }

    @Override
    protected void populateNearCache(NearCacheTestContext<Integer, Integer, ?, ?> context, int size) {
        IMap<Integer, Integer> map = ((TransactionalMapDataStructureAdapter<Integer, Integer>) context.nearCacheAdapter).getMap();
        for (int i = 0; i < size; i++) {
            map.get(i);
            map.get(i);
        }
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected Config getConfig(boolean withNearCache) {
        Config config = getConfig();

        if (withNearCache) {
            config.getMapConfig(DEFAULT_NEAR_CACHE_NAME).setNearCacheConfig(nearCacheConfig);
        }
        return config;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        Config configWithNearCache = getConfig(true);

        HecatonInstance nearCacheInstance = hecatonFactory.newHecatonInstance(configWithNearCache);
        nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(nearCacheInstance);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        RepairingTask repairingTask = ((MapNearCacheManager) nearCacheManager).getRepairingTask();

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(nearCacheInstance))
                .setNearCacheInstance(nearCacheInstance)
                .setNearCacheAdapter(new TransactionalMapDataStructureAdapter<>(nearCacheInstance, DEFAULT_NEAR_CACHE_NAME))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setRepairingTask(repairingTask);
    }
}
