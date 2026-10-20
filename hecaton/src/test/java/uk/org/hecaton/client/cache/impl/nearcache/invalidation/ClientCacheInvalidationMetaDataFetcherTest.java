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

package uk.org.hecaton.client.cache.impl.nearcache.invalidation;

import uk.org.hecaton.cache.impl.CacheEventHandler;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataContainer;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingHandler;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.spi.CachingProvider;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.cache.impl.ICacheService.SERVICE_NAME;
import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static uk.org.hecaton.internal.util.RandomPicker.getInt;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheInvalidationMetaDataFetcherTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void fetches_sequence_and_uuid() {
        String cacheName = "test";
        int partition = 1;
        long givenSequence = getInt(1, MAX_VALUE);
        UUID givenUuid = UuidUtil.newUnsecureUUID();

        RepairingTask repairingTask = getRepairingTask(cacheName, partition, givenSequence, givenUuid);
        InvalidationMetaDataFetcher invalidationMetaDataFetcher = repairingTask.getInvalidationMetaDataFetcher();
        ConcurrentMap<String, RepairingHandler> handlers = repairingTask.getHandlers();
        invalidationMetaDataFetcher.fetchMetadata(handlers);

        RepairingHandler repairingHandler = handlers.get(getPrefixedName(cacheName));
        MetaDataContainer metaDataContainer = repairingHandler.getMetaDataContainer(partition);

        UUID foundUuid = metaDataContainer.getUuid();
        long foundSequence = metaDataContainer.getSequence();

        assertEquals(givenSequence, foundSequence);
        assertEquals(givenUuid, foundUuid);
    }

    @SuppressWarnings("unchecked")
    private RepairingTask getRepairingTask(String cacheName, int partition, long givenSequence, UUID givenUuid) {
        HecatonInstance member = factory.newHecatonInstance();
        distortRandomPartitionSequence(getPrefixedName(cacheName), partition, givenSequence, member);
        distortRandomPartitionUuid(partition, givenUuid, member);

        ClientConfig clientConfig = new ClientConfig().addNearCacheConfig(new NearCacheConfig(cacheName));
        HecatonClientProxy client = (HecatonClientProxy) factory.newHecatonClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);
        Cache<Integer, Integer> clientCache = clientCachingProvider.getCacheManager().createCache(cacheName, newCacheConfig());

        ClientContext clientContext = ((ClientProxy) clientCache).getContext();
        return clientContext.getRepairingTask(SERVICE_NAME);
    }

    private void distortRandomPartitionSequence(String cacheName, int partition, long sequence, HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();
        metaDataGenerator.setCurrentSequence(cacheName, partition, sequence);
    }

    private void distortRandomPartitionUuid(int partition, UUID uuid, HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();
        metaDataGenerator.setUuid(partition, uuid);
    }

    private CacheConfig newCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        return new CacheConfig()
                .setEvictionConfig(evictionConfig);
    }

    private String getPrefixedName(String cacheName) {
        return "/hz/" + cacheName;
    }
}
