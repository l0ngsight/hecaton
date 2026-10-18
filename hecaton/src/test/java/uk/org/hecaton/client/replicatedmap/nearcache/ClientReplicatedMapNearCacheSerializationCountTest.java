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

package uk.org.hecaton.client.replicatedmap.nearcache;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.proxy.ClientReplicatedMapProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.adapter.DataStructureAdapter.DataStructureMethods;
import uk.org.hecaton.internal.adapter.DataStructureAdapterMethod;
import uk.org.hecaton.internal.adapter.ReplicatedMapDataStructureAdapter;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.AbstractNearCacheSerializationCountTest;
import uk.org.hecaton.internal.nearcache.impl.NearCacheSerializationCountConfigBuilder;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContext;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContextBuilder;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
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
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link ReplicatedMap} on Hecaton clients.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientReplicatedMapNearCacheSerializationCountTest extends AbstractNearCacheSerializationCountTest<Data, String> {

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
    public InMemoryFormat replicatedMapInMemoryFormat;

    @Parameter(value = 6)
    public InMemoryFormat nearCacheInMemoryFormat;

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Parameters(name = "method:{0} replicatedMapFormat:{5} nearCacheFormat:{6} serializeKeys:{7}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(0, 1, 1), BINARY, BINARY},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT},

                // FIXME: we should not serialize the value twice
                {GET, newInt(1, 1, 1), newInt(1, 1, 1), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null},
                {GET, newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 2, 0), newInt(1, 1, 1), OBJECT, BINARY},
                {GET, newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT},
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
            nearCacheConfig = createNearCacheConfig(nearCacheInMemoryFormat, false)
                    // we have to disable invalidations, otherwise there will be an non-deterministic serialization in a listener
                    .setInvalidateOnChange(false);
        }
    }

    @After
    public void tearDown() {
        hecatonFactory.shutdownAll();
    }

    @Override
    protected void addConfiguration(NearCacheSerializationCountConfigBuilder configBuilder) {
        configBuilder.append(method);
        configBuilder.append(replicatedMapInMemoryFormat);
        configBuilder.append(nearCacheInMemoryFormat);
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(ReplicatedMapDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "1");
        config.getReplicatedMapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(replicatedMapInMemoryFormat);
        prepareSerializationConfig(config.getSerializationConfig());

        HecatonInstance member = hecatonFactory.newHecatonInstance(config);
        ReplicatedMap<K, V> memberMap = member.getReplicatedMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(member)
                .setDataAdapter(new ReplicatedMapDataStructureAdapter<>(memberMap))
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder.build();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        ClientConfig clientConfig = getClientConfig();
        if (nearCacheConfig != null) {
            clientConfig.addNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(clientConfig.getSerializationConfig());

        HecatonClientProxy client = (HecatonClientProxy) hecatonFactory.newHecatonClient(clientConfig);
        ReplicatedMap<K, V> clientMap = client.getReplicatedMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = ((ClientReplicatedMapProxy) clientMap).getContext()
                .getNearCacheManager(clientMap.getServiceName());
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ReplicatedMapDataStructureAdapter<>(clientMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager);
    }
}
