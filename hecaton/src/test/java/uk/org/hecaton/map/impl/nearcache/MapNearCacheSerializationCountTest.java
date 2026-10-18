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
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.adapter.DataStructureAdapter.DataStructureMethods;
import uk.org.hecaton.internal.adapter.DataStructureAdapterMethod;
import uk.org.hecaton.internal.adapter.IMapDataStructureAdapter;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.AbstractNearCacheSerializationCountTest;
import uk.org.hecaton.internal.nearcache.impl.NearCacheSerializationCountConfigBuilder;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContext;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContextBuilder;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.internal.adapter.DataStructureAdapter.DataStructureMethods.GET;
import static uk.org.hecaton.internal.adapter.DataStructureAdapter.DataStructureMethods.GET_ALL;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getMapNearCacheManager;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link IMap} on Hecaton members.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class MapNearCacheSerializationCountTest extends AbstractNearCacheSerializationCountTest<Data, String> {

    @Parameter
    public DataStructureMethods method;

    @Parameter(value = 1)
    public int[] keySerializationCounts;

    @Parameter(value = 2)
    public int[] keyDeserializationCounts;

    @Parameter(value = 3)
    public int[] valueSerializationCounts;

    @Parameter(value = 4)
    public int[] valueDeserializationCounts;

    @Parameter(value = 5)
    public InMemoryFormat mapInMemoryFormat;

    @Parameter(value = 6)
    public InMemoryFormat nearCacheInMemoryFormat;

    @Parameter(value = 7)
    public Boolean invalidateOnChange;

    @Parameter(value = 8)
    public Boolean serializeKeys;

    private final TestHecatonInstanceFactory hecatonFactory = createHecatonInstanceFactory(2);

    @Parameters(name = "method:{0} mapFormat:{5} nearCacheFormat:{6} invalidateOnChange:{7} serializeKeys:{8}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null, null, null},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, false},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false, false},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, true, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, true, false},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, false, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, false, false},

                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null, null, null},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, true, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, true, false},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, false, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, false, false},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, true, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, true, false},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, true},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, false},

                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null, null, null},
                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, true},
                {GET_ALL, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, false},

                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null, null, null},
                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, true},
                {GET_ALL, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, false},
        });
    }

    @Before
    public void setUp() {
        testMethod = method;
        expectedKeySerializationCounts = keySerializationCounts;
        expectedKeyDeserializationCounts = keyDeserializationCounts;
        expectedValueSerializationCounts = valueSerializationCounts;
        expectedValueDeserializationCounts = valueDeserializationCounts;
        if (nearCacheInMemoryFormat != null) {
            nearCacheConfig = createNearCacheConfig(nearCacheInMemoryFormat, serializeKeys)
                    .setInvalidateOnChange(invalidateOnChange)
                    .setCacheLocalEntries(true);
        }
    }

    @After
    public void tearDown() {
        hecatonFactory.shutdownAll();
    }

    @Override
    protected void addConfiguration(NearCacheSerializationCountConfigBuilder configBuilder) {
        configBuilder.append(method);
        configBuilder.append(mapInMemoryFormat);
        configBuilder.append(nearCacheInMemoryFormat);
        configBuilder.append(invalidateOnChange);
        configBuilder.append(serializeKeys);
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(IMapDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        HecatonInstance dataMember = hecatonFactory.newHecatonInstance(getConfig(false));
        IMap<K, V> dataMap = dataMember.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(dataMember)
                .setDataAdapter(new IMapDataStructureAdapter<>(dataMap))
                .setHasLocalData(true)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder.build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    private Config getConfig(boolean withNearCache) {
        Config config = getConfig()
                // we don't want to have the invalidations from the initial population being sent during this test
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(Integer.MAX_VALUE))
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE))
                .setProperty(PARTITION_COUNT.getName(), "1")
                .setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "1");
        MapConfig mapConfig = config.getMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(mapInMemoryFormat)
                .setBackupCount(0)
                .setAsyncBackupCount(0);
        if (withNearCache && nearCacheConfig != null) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(config.getSerializationConfig());
        return config;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        HecatonInstance nearCacheMember = hecatonFactory.newHecatonInstance(getConfig(true));
        IMap<K, V> nearCacheMap = nearCacheMember.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(nearCacheMember);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(nearCacheMember))
                .setNearCacheInstance(nearCacheMember)
                .setNearCacheAdapter(new IMapDataStructureAdapter<>(nearCacheMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager);
    }
}
