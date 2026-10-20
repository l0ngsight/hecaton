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

import uk.org.hecaton.cache.impl.HecatonServerCacheManager;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.adapter.DataStructureAdapter;
import uk.org.hecaton.internal.adapter.DataStructureLoader;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonTestSupport;

import javax.cache.CacheManager;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertNotEquals;

/**
 * Builder for {@link NearCacheTestContext}.
 */
public class NearCacheTestContextBuilder<K, V, NK, NV> extends HecatonTestSupport {

    private NearCacheConfig nearCacheConfig;
    private SerializationService serializationService;

    private HecatonInstance nearCacheInstance;
    private HecatonInstance dataInstance;
    private DataStructureAdapter<K, V> nearCacheAdapter;
    private DataStructureAdapter<K, V> dataAdapter;

    private NearCache<NK, NV> nearCache;
    private NearCacheManager nearCacheManager;
    private CacheManager cacheManager;
    private HecatonServerCacheManager memberCacheManager;

    private boolean hasLocalData;
    private DataStructureLoader loader;

    private RepairingTask repairingTask;

    public NearCacheTestContextBuilder(NearCacheConfig nearCacheConfig, SerializationService serializationService) {
        this.nearCacheConfig = nearCacheConfig;
        this.serializationService = serializationService;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setNearCacheInstance(HecatonInstance nearCacheInstance) {
        this.nearCacheInstance = nearCacheInstance;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setDataInstance(HecatonInstance dataInstance) {
        this.dataInstance = dataInstance;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setNearCacheAdapter(DataStructureAdapter<K, V> nearCacheAdapter) {
        this.nearCacheAdapter = nearCacheAdapter;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setDataAdapter(DataStructureAdapter<K, V> dataAdapter) {
        this.dataAdapter = dataAdapter;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setNearCache(NearCache<NK, NV> nearCache) {
        this.nearCache = nearCache;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setNearCacheManager(NearCacheManager nearCacheManager) {
        this.nearCacheManager = nearCacheManager;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setMemberCacheManager(HecatonServerCacheManager memberCacheManager) {
        this.memberCacheManager = memberCacheManager;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setHasLocalData(boolean hasLocalData) {
        this.hasLocalData = hasLocalData;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setLoader(DataStructureLoader loader) {
        this.loader = loader;
        return this;
    }

    public NearCacheTestContextBuilder<K, V, NK, NV> setRepairingTask(RepairingTask repairingTask) {
        this.repairingTask = repairingTask;
        return this;
    }

    public NearCacheTestContext<K, V, NK, NV> build() {
        checkNotNull(serializationService, "serializationService cannot be null!");

        assertNotEquals("nearCacheInstance and dataInstance have to be different instances", nearCacheInstance, dataInstance);
        assertNotEquals("nearCacheAdapter and dataAdapter have to be different instances", nearCacheAdapter, dataAdapter);

        NearCacheTestContext<K, V, NK, NV> context = new NearCacheTestContext<>(
                nearCacheConfig,
                serializationService,
                nearCacheInstance,
                dataInstance,
                nearCacheAdapter,
                dataAdapter,
                nearCache,
                nearCacheManager,
                cacheManager,
                memberCacheManager,
                hasLocalData,
                loader,
                repairingTask);

        warmUpPartitions(context.dataInstance, context.nearCacheInstance);
        waitAllForSafeState(context.dataInstance, context.nearCacheInstance);

        return context;
    }
}
