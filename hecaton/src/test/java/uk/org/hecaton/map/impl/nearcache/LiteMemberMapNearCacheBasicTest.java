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

package uk.org.hecaton.map.impl.nearcache;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.adapter.DataStructureAdapterMethod;
import uk.org.hecaton.internal.adapter.IMapDataStructureAdapter;
import uk.org.hecaton.internal.adapter.IMapMapStore;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.AbstractNearCacheBasicTest;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContext;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContextBuilder;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static uk.org.hecaton.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getMapNearCacheManager;
import static uk.org.hecaton.map.impl.nearcache.MapNearCacheBasicTest.addMapStoreConfig;
import static uk.org.hecaton.test.Accessors.getSerializationService;

/**
 * Basic Near Cache tests for {@link IMap} on Hecaton Lite members.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LiteMemberMapNearCacheBasicTest extends AbstractNearCacheBasicTest<Data, String> {

    private final TestHecatonInstanceFactory hecatonFactory = createHecatonInstanceFactory(2);

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(DEFAULT_MEMORY_FORMAT, DEFAULT_SERIALIZE_KEYS);
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(IMapDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean loaderEnabled) {
        IMapMapStore mapStore = loaderEnabled ? new IMapMapStore() : null;
        Config config = getConfig(mapStore, false);

        HecatonInstance member = hecatonFactory.newHecatonInstance(config);
        IMap<K, V> memberMap = member.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMapDataStructureAdapter<K, V> dataAdapter = new IMapDataStructureAdapter<>(memberMap);

        // wait until the initial load is done
        dataAdapter.waitUntilLoaded();

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder(mapStore);
        return builder
                .setDataInstance(member)
                .setDataAdapter(dataAdapter)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder(null);
        return builder.build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected Config getConfig(IMapMapStore mapStore, boolean liteMember) {
        Config config = getConfig()
                .setLiteMember(liteMember);
        config.setProperty(NearCache.PROP_EXPIRATION_TASK_INITIAL_DELAY_SECONDS, "0");
        config.setProperty(NearCache.PROP_EXPIRATION_TASK_PERIOD_SECONDS, "1");

        MapConfig mapConfig = config.getMapConfig(DEFAULT_NEAR_CACHE_NAME);
        addMapStoreConfig(mapStore, mapConfig);
        if (liteMember) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }

        return config;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder(IMapMapStore mapStore) {
        Config configWithNearCache = getConfig(mapStore, true);

        HecatonInstance liteMember = hecatonFactory.newHecatonInstance(configWithNearCache);
        IMap<K, V> liteMemberMap = liteMember.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(liteMember);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(liteMember))
                .setNearCacheInstance(liteMember)
                .setNearCacheAdapter(new IMapDataStructureAdapter<>(liteMemberMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setLoader(mapStore);
    }
}
