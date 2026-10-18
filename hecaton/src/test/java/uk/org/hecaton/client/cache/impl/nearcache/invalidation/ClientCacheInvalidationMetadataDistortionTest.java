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
import uk.org.hecaton.client.cache.impl.nearcache.ClientNearCacheTestSupport;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static uk.org.hecaton.internal.util.RandomPicker.getInt;
import static uk.org.hecaton.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientCacheInvalidationMetadataDistortionTest extends ClientNearCacheTestSupport {

    private static final int CACHE_SIZE = 100000;

    private final AtomicBoolean stopTest = new AtomicBoolean();

    @Test
    public void ensure_nearCachedClient_and_member_data_sync_eventually() {
        final Config config = createConfig();
        final HecatonInstance member = hecatonFactory.newHecatonInstance(config);

        CachingProvider provider = createServerCachingProvider(serverInstance);
        CacheManager serverCacheManager = provider.getCacheManager();

        // populated from member
        final Cache<Integer, Integer> memberCache = serverCacheManager.createCache(DEFAULT_CACHE_NAME, createCacheConfig(BINARY));
        for (int i = 0; i < CACHE_SIZE; i++) {
            memberCache.put(i, i);
        }

        ClientConfig clientConfig = createClientConfig();
        clientConfig.addNearCacheConfig(createNearCacheConfig(BINARY));
        HecatonClientProxy client = (HecatonClientProxy) hecatonFactory.newHecatonClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);

        final Cache<Integer, Integer> clientCache = clientCachingProvider.getCacheManager().createCache(
                DEFAULT_CACHE_NAME, createCacheConfig(BINARY));

        Thread populateNearCache = new Thread(() -> {
            while (!stopTest.get()) {
                for (int i = 0; i < CACHE_SIZE; i++) {
                    clientCache.get(i);
                }
            }
        });

        Thread distortSequence = new Thread(() -> {
            while (!stopTest.get()) {
                distortRandomPartitionSequence(DEFAULT_CACHE_NAME, member);
                sleepSeconds(1);
            }
        });

        Thread distortUuid = new Thread(() -> {
            while (!stopTest.get()) {
                distortRandomPartitionUuid(member);
                sleepSeconds(5);
            }
        });

        Thread put = new Thread(() -> {
            // change some data
            while (!stopTest.get()) {
                int key = getInt(CACHE_SIZE);
                int value = getInt(Integer.MAX_VALUE);
                memberCache.put(key, value);
                sleepAtLeastMillis(100);
            }
        });

        // start threads
        put.start();
        populateNearCache.start();
        distortSequence.start();
        distortUuid.start();

        sleepSeconds(60);

        // stop threads
        stopTest.set(true);
        assertJoinable(distortUuid, distortSequence, populateNearCache, put);

        assertTrueEventually(() -> {
            for (int i = 0; i < CACHE_SIZE; i++) {
                Integer valueSeenFromMember = memberCache.get(i);
                Integer valueSeenFromClient = clientCache.get(i);

                assertEquals(valueSeenFromMember, valueSeenFromClient);
            }
        });
    }

    @Override
    protected Config createConfig() {
        return super.createConfig()
                .setProperty(PARTITION_COUNT.getName(), "271")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10");
    }

    @Override
    protected ClientConfig createClientConfig() {
        return new ClientConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CacheConfig<Integer, Integer> createCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        CacheConfig<Integer, Integer> cacheConfig = super.createCacheConfig(inMemoryFormat);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        return super.createNearCacheConfig(inMemoryFormat)
                .setInvalidateOnChange(true)
                .setEvictionConfig(evictionConfig);
    }

    private void distortRandomPartitionSequence(String mapName, HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();
        InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();

        int randomPartition = getInt(partitionCount);
        int randomSequence = getInt(MAX_VALUE);
        metaDataGenerator.setCurrentSequence(mapName, randomPartition, randomSequence);
    }

    private void distortRandomPartitionUuid(HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();

        UUID uuid = UuidUtil.newUnsecureUUID();
        int randomPartition = getInt(partitionCount);
        metaDataGenerator.setUuid(randomPartition, uuid);
    }
}
