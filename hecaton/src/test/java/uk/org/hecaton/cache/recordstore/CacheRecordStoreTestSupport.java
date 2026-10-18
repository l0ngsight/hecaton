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

package uk.org.hecaton.cache.recordstore;

import uk.org.hecaton.cache.impl.AbstractCacheService;
import uk.org.hecaton.cache.impl.CacheRecordStore;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import org.junit.After;
import org.junit.Before;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import java.util.Collections;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

public abstract class CacheRecordStoreTestSupport
        extends HecatonTestSupport {

    protected static final String CACHE_NAME_PREFIX = "hz:";
    protected static final String DEFAULT_CACHE_NAME = "MyCache";
    protected static final int DEFAULT_PARTITION_ID = 1;
    protected static final int CACHE_RECORD_COUNT = 50;

    protected TestHecatonInstanceFactory factory;
    protected HecatonInstance hz;

    @Before
    public void init() {
        factory = new TestHecatonInstanceFactory(1);
        hz = factory.newHecatonInstance(createConfig());
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    protected Config createConfig() {
        return new Config();
    }

    protected CacheConfig createCacheConfig(String cacheName, InMemoryFormat inMemoryFormat) {
        return new CacheConfig()
                .setName(cacheName)
                .setManagerPrefix(CACHE_NAME_PREFIX)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected ICacheService getCacheService(HecatonInstance instance) {
        return getNodeEngineImpl(instance).getService(ICacheService.SERVICE_NAME);
    }

    protected ICacheRecordStore createCacheRecordStore(HecatonInstance instance, String cacheName,
                                                       int partitionId, InMemoryFormat inMemoryFormat) {
        NodeEngine nodeEngine = getNodeEngineImpl(instance);
        ICacheService cacheService = getCacheService(instance);
        CacheConfig cacheConfig = createCacheConfig(cacheName, inMemoryFormat);
        cacheService.putCacheConfigIfAbsent(cacheConfig);
        return new CacheRecordStore(CACHE_NAME_PREFIX + cacheName, partitionId, nodeEngine,
                (AbstractCacheService) cacheService);
    }

    protected ICacheRecordStore createCacheRecordStore(HecatonInstance instance, InMemoryFormat inMemoryFormat) {
        return createCacheRecordStore(instance, DEFAULT_CACHE_NAME, DEFAULT_PARTITION_ID, inMemoryFormat);
    }

    protected ICacheRecordStore createCacheRecordStore(InMemoryFormat inMemoryFormat) {
        return createCacheRecordStore(hz, DEFAULT_CACHE_NAME, DEFAULT_PARTITION_ID, inMemoryFormat);
    }

    protected void putAndGetFromCacheRecordStore(ICacheRecordStore cacheRecordStore, InMemoryFormat inMemoryFormat) {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
            cacheRecordStore.put(serializationService.toData(i), "value-" + i, null, null, -1);
        }

        if (inMemoryFormat == InMemoryFormat.BINARY || inMemoryFormat == InMemoryFormat.NATIVE) {
            for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
                assertTrue(Data.class.isAssignableFrom(
                        cacheRecordStore.get(serializationService.toData(i), null).getClass()));
            }
        } else if (inMemoryFormat == InMemoryFormat.OBJECT) {
            for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
                assertTrue(String.class.isAssignableFrom(
                        cacheRecordStore.get(serializationService.toData(i), null).getClass()));
            }
        } else {
            throw new IllegalArgumentException("Unsupported in-memory format: " + inMemoryFormat);
        }
    }

    protected void putAndSetExpiryPolicyFromRecordStore(ICacheRecordStore cacheRecordStore, InMemoryFormat inMemoryFormat) {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        ExpiryPolicy expiryPolicy = new TouchedExpiryPolicy(Duration.ETERNAL);
        Data policyData = serializationService.toData(expiryPolicy);

        for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
            Data keyData = serializationService.toData(i);

            cacheRecordStore.put(keyData, "value-" + i, null, null, -1);
            cacheRecordStore.setExpiryPolicy(Collections.singleton(keyData), policyData, null);
        }

        if (inMemoryFormat == InMemoryFormat.BINARY || inMemoryFormat == InMemoryFormat.NATIVE) {
            for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
                assertTrue(Data.class.isAssignableFrom(
                        cacheRecordStore.getExpiryPolicy(serializationService.toData(i)).getClass()));
            }
        } else if (inMemoryFormat == InMemoryFormat.OBJECT) {
            for (int i = 0; i < CACHE_RECORD_COUNT; i++) {
                assertTrue(ExpiryPolicy.class.isAssignableFrom(
                        cacheRecordStore.getExpiryPolicy(serializationService.toData(i)).getClass()));
            }
        } else {
            throw new IllegalArgumentException("Unsupported in-memory format: " + inMemoryFormat);
        }


    }

}
