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

package uk.org.hecaton.client.map.impl.querycache.subscriber;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.map.impl.querycache.QueryCacheConfigurator;
import uk.org.hecaton.map.impl.querycache.QueryCacheEventService;
import uk.org.hecaton.map.impl.querycache.subscriber.AbstractQueryCacheConfigurator;

import java.util.Map;

/**
 * Client side implementation of {@link QueryCacheConfigurator}.
 *
 * @see QueryCacheConfigurator
 */
public class ClientQueryCacheConfigurator extends AbstractQueryCacheConfigurator {

    private final ClientConfig clientConfig;
    private final ClassLoader classLoader;

    public ClientQueryCacheConfigurator(ClientConfig clientConfig,
                                        QueryCacheEventService eventService) {
        super(eventService);
        this.clientConfig = clientConfig;
        this.classLoader = clientConfig.getClassLoader();
    }

    @Override
    public QueryCacheConfig getOrCreateConfiguration(String mapName, String cacheName, String cacheId) {
        QueryCacheConfig config = clientConfig.getOrCreateQueryCacheConfig(mapName, cacheName);

        setPredicateImpl(config, classLoader);
        setEntryListener(mapName, cacheId, config, classLoader);

        return config;
    }

    @Override
    public QueryCacheConfig getOrNull(String mapName, String cacheName, String cacheId) {
        QueryCacheConfig config = clientConfig.getOrNullQueryCacheConfig(mapName, cacheName);
        if (config != null) {
            setPredicateImpl(config, classLoader);
            setEntryListener(mapName, cacheId, config, classLoader);
        }

        return config;
    }

    @Override
    public void removeConfiguration(String mapName, String cacheName) {
        Map<String, Map<String, QueryCacheConfig>> allQueryCacheConfig = clientConfig.getQueryCacheConfigs();
        Map<String, QueryCacheConfig> mapQueryCacheConfig = allQueryCacheConfig.get(mapName);
        if (mapQueryCacheConfig == null || mapQueryCacheConfig.isEmpty()) {
            return;
        }
        mapQueryCacheConfig.remove(cacheName);
    }

}
