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

package uk.org.hecaton.client.cache.impl;

import uk.org.hecaton.cache.CacheNotExistsException;
import uk.org.hecaton.cache.HecatonCacheManager;
import uk.org.hecaton.client.cache.impl.nearcache.NearCachedClientCacheProxy;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.impl.spi.ClientProxyFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.NearCacheConfig;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClientCacheProxyFactory implements ClientProxyFactory {

    private final HecatonClientInstanceImpl client;
    private final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<>();

    public ClientCacheProxyFactory(HecatonClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public ClientProxy create(String id, ClientContext context) {
        CacheConfig cacheConfig = findCacheConfig(id);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + id + " is already destroyed or not created yet");
        }
        NearCacheConfig nearCacheConfig = client.getClientConfig().getNearCacheConfig(cacheConfig.getName());
        if (nearCacheConfig != null) {
            return new NearCachedClientCacheProxy(cacheConfig, context);
        }
        return new ClientCacheProxy(cacheConfig, context);
    }

    public void recreateCachesOnCluster() {
        for (CacheConfig cacheConfig : configs.values()) {
            ClientCacheHelper.createCacheConfig(client, cacheConfig, true);
        }
    }

    void addCacheConfig(String cacheName, CacheConfig cacheConfig) {
        configs.putIfAbsent(cacheName, cacheConfig);
    }

    void removeCacheConfig(String cacheName) {
        configs.remove(cacheName);
    }

    CacheConfig getCacheConfig(String cacheName) {
        return configs.get(cacheName);
    }

    Set<Map.Entry<String, CacheConfig>> configs() {
        return configs.entrySet();
    }

    private CacheConfig findCacheConfig(String cacheName) {
        CacheConfig cacheConfig = configs.get(cacheName);
        if (cacheConfig != null) {
            return cacheConfig;
        }
        // otherwise, request it from server
        String simpleCacheName = cacheName.substring(HecatonCacheManager.CACHE_MANAGER_PREFIX.length());
        cacheConfig = ClientCacheHelper.getCacheConfig(client, cacheName, simpleCacheName);
        if (cacheConfig != null) {
            configs.putIfAbsent(cacheName, cacheConfig);
        }
        return cacheConfig;
    }
}
