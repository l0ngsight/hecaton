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

package uk.org.hecaton.client.map.impl.nearcache;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.proxy.ClientMapProxy;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.adapter.DataStructureAdapter;
import uk.org.hecaton.internal.adapter.IMapDataStructureAdapter;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.AbstractNearCachePreloaderTest;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContext;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContextBuilder;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static uk.org.hecaton.config.NearCacheConfig.DEFAULT_INVALIDATE_ON_CHANGE;
import static uk.org.hecaton.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static uk.org.hecaton.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.test.Accessors.getSerializationService;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class ClientMapNearCachePreloaderTest extends AbstractNearCachePreloaderTest<Data, String> {

    protected final File storeFile = new File("nearCache-" + defaultNearCache + ".store").getAbsoluteFile();
    protected final File storeLockFile = new File(storeFile.getName() + ".lock").getAbsoluteFile();

    @Before
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(DEFAULT_MEMORY_FORMAT,
                DEFAULT_SERIALIZE_KEYS, DEFAULT_INVALIDATE_ON_CHANGE, KEY_COUNT, storeFile.getParent());
    }

    @Override
    protected File getStoreFile() {
        return storeFile;
    }

    @Override
    protected File getStoreLockFile() {
        return storeLockFile;
    }

    @Override
    protected <K, V> DataStructureAdapter<K, V> getDataStructure(NearCacheTestContext<K, V, Data, String> context, String name) {
        IMap<K, V> map = context.nearCacheInstance.getMap(name);
        return new IMapDataStructureAdapter<>(map);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean createNearCacheInstance) {
        Config config = getConfig();

        HecatonInstance member = hecatonFactory.newHecatonInstance(config);
        IMap<K, V> memberMap = member.getMap(nearCacheConfig.getName());
        IMapDataStructureAdapter<K, V> dataAdapter = new IMapDataStructureAdapter<>(memberMap);

        if (createNearCacheInstance) {
            NearCacheTestContextBuilder<K, V, Data, String> contextBuilder = createNearCacheContextBuilder();
            return contextBuilder
                    .setDataInstance(member)
                    .setDataAdapter(dataAdapter)
                    .build();
        }
        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(member))
                .setDataInstance(member)
                .setDataAdapter(dataAdapter)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        NearCacheTestContextBuilder<K, V, Data, String> contextBuilder = createNearCacheContextBuilder();
        return contextBuilder.build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HecatonClientProxy client = (HecatonClientProxy) hecatonFactory.newHecatonClient(clientConfig);
        IMap<K, V> clientMap = client.getMap(nearCacheConfig.getName());

        NearCacheManager nearCacheManager = ((ClientMapProxy) clientMap).getContext()
                .getNearCacheManager(clientMap.getServiceName());
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(nearCacheConfig.getName());

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new IMapDataStructureAdapter<>(clientMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager);
    }
}
