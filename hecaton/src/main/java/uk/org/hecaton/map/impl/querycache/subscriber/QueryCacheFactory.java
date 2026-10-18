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

package uk.org.hecaton.map.impl.querycache.subscriber;

import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.listener.MapListener;
import uk.org.hecaton.internal.util.ConcurrencyUtil;
import uk.org.hecaton.internal.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Static factory for simple {@link uk.org.hecaton.map.QueryCache QueryCache} implementations.
 */
public class QueryCacheFactory {

    /**
     * Constructor for an instance of {@link uk.org.hecaton.map.QueryCache QueryCache}.
     */
    private static class InternalQueryCacheConstructor implements ConstructorFunction<String, InternalQueryCache> {

        private final QueryCacheRequest request;

        InternalQueryCacheConstructor(QueryCacheRequest request) {
            this.request = request;
        }

        @Override
        public InternalQueryCache createNew(String cacheId) {
            IMap delegate = request.getMap();
            String mapName = request.getMapName();
            String cacheName = request.getCacheName();
            QueryCacheContext context = request.getContext();
            QueryCacheConfig queryCacheConfig = request.getQueryCacheConfig();

            DefaultQueryCache queryCache = new DefaultQueryCache(cacheId, cacheName, queryCacheConfig, delegate, context);

            MapListener listener = request.getListener();
            if (listener != null) {
                // this is users listener which can be given as a parameter
                // when calling `IMap.getQueryCache` method
                request.getContext()
                        .getSubscriberContext()
                        .getEventService().addListener(mapName, cacheId, listener);
            }

            return queryCache;
        }
    }

    private final ConcurrentMap<String, InternalQueryCache> internalQueryCaches;

    public QueryCacheFactory() {
        this.internalQueryCaches = new ConcurrentHashMap<>();
    }

    public InternalQueryCache create(QueryCacheRequest request, String cacheId) {
        return ConcurrencyUtil.getOrPutIfAbsent(internalQueryCaches,
                cacheId, new InternalQueryCacheConstructor(request));
    }

    public boolean remove(InternalQueryCache queryCache) {
        return internalQueryCaches.remove(queryCache.getCacheId(), queryCache);
    }

    public InternalQueryCache getOrNull(String cacheId) {
        return internalQueryCaches.get(cacheId);
    }

    // only used for testing
    public int getQueryCacheCount() {
        return internalQueryCaches.size();
    }
}
