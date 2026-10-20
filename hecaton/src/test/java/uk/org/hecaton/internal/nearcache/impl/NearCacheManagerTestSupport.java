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

package uk.org.hecaton.internal.nearcache.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.spi.properties.HecatonProperties;
import org.junit.Before;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class NearCacheManagerTestSupport extends CommonNearCacheTestSupport {

    private static final int DEFAULT_NEAR_CACHE_COUNT = 5;

    protected abstract NearCacheManager createNearCacheManager();

    protected SerializationService ss;
    protected HecatonProperties properties;
    protected ExecutionService executionService;

    @Before
    public void setUp() {
        Config config = getConfig();
        HecatonInstance instance = createHecatonInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        properties = nodeEngineImpl.getProperties();
        ss = nodeEngineImpl.getSerializationService();
        executionService = nodeEngineImpl.getExecutionService();
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    void doCreateAndGetNearCache() {
        NearCacheManager nearCacheManager = createNearCacheManager();

        assertNull(nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME));

        NearCache createdNearCache1 = createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME);
        assertNotNull(createdNearCache1);

        NearCache createdNearCache2 = createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME);
        assertNotNull(createdNearCache2);
        assertEquals(createdNearCache1, createdNearCache2);

        Collection<NearCache> nearCaches = nearCacheManager.listAllNearCaches();
        assertEquals(1, nearCaches.size());
        assertEquals(createdNearCache1, nearCaches.iterator().next());
    }

    void doListNearCaches() {
        NearCacheManager nearCacheManager = createNearCacheManager();

        Set<String> nearCacheNames = new HashSet<>();

        Collection<NearCache> nearCaches1 = nearCacheManager.listAllNearCaches();
        assertEquals(0, nearCaches1.size());

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            String nearCacheName = DEFAULT_NEAR_CACHE_NAME + "-" + i;
            createNearCache(nearCacheManager, nearCacheName);
            nearCacheNames.add(nearCacheName);
        }

        Collection<NearCache> nearCaches2 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches2.size());

        for (NearCache nearCache : nearCaches2) {
            assertContains(nearCacheNames, nearCache.getName());
        }
    }

    void doClearNearCacheAndClearAllNearCaches() {
        NearCacheManager nearCacheManager = createNearCacheManager();
        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME + "-" + i);
        }

        Collection<NearCache> nearCaches1 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches1.size());

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            assertTrue(nearCacheManager.clearNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + i));
        }

        Collection<NearCache> nearCaches2 = nearCacheManager.listAllNearCaches();
        // clear doesn't remove Near Cache, just clears it
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches2.size());

        nearCacheManager.clearAllNearCaches();
        Collection<NearCache> nearCaches3 = nearCacheManager.listAllNearCaches();
        // clear all doesn't remove Near Caches, just clears them
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches3.size());

        assertFalse(nearCacheManager.clearNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + DEFAULT_NEAR_CACHE_COUNT));
    }

    void doDestroyNearCacheAndDestroyAllNearCaches() {
        NearCacheManager nearCacheManager = createNearCacheManager();

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME + "-" + i);
        }

        Collection<NearCache> nearCaches1 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches1.size());

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            assertTrue(nearCacheManager.destroyNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + i));
        }

        Collection<NearCache> nearCaches2 = nearCacheManager.listAllNearCaches();
        // destroy also removes Near Cache
        assertEquals(0, nearCaches2.size());

        assertFalse(nearCacheManager.clearNearCache(DEFAULT_NEAR_CACHE_NAME + "-" + DEFAULT_NEAR_CACHE_COUNT));

        for (int i = 0; i < DEFAULT_NEAR_CACHE_COUNT; i++) {
            createNearCache(nearCacheManager, DEFAULT_NEAR_CACHE_NAME + "-" + i);
        }

        Collection<NearCache> nearCaches3 = nearCacheManager.listAllNearCaches();
        assertEquals(DEFAULT_NEAR_CACHE_COUNT, nearCaches3.size());

        nearCacheManager.destroyAllNearCaches();
        Collection<NearCache> nearCaches4 = nearCacheManager.listAllNearCaches();
        // destroy all also removes Near Caches
        assertEquals(0, nearCaches4.size());
    }

    // overridden in EE test
    protected NearCache createNearCache(NearCacheManager nearCacheManager, String name, InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(name, inMemoryFormat);
        return nearCacheManager.getOrCreateNearCache(name, nearCacheConfig);
    }

    protected NearCache createNearCache(NearCacheManager nearCacheManager, String name) {
        return createNearCache(nearCacheManager, name, NearCacheConfig.DEFAULT_MEMORY_FORMAT);
    }
}
