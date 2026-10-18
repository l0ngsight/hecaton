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

import uk.org.hecaton.config.EntryListenerConfig;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.PredicateConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.map.impl.EntryEventFilter;
import uk.org.hecaton.map.impl.querycache.QueryCacheConfigurator;
import uk.org.hecaton.map.impl.querycache.QueryCacheEventService;
import uk.org.hecaton.map.listener.MapListener;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;
import uk.org.hecaton.internal.util.ExceptionUtil;

import java.util.EventListener;

import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmpty;

/**
 * Abstract {@link QueryCacheConfigurator} includes common functionality.
 *
 * @see QueryCacheConfigurator
 */
public abstract class AbstractQueryCacheConfigurator implements QueryCacheConfigurator {
    private final QueryCacheEventService eventService;

    public AbstractQueryCacheConfigurator(QueryCacheEventService eventService) {
        this.eventService = eventService;
    }

    protected void setEntryListener(String mapName, String cacheId, QueryCacheConfig config, ClassLoader loader) {
        for (EntryListenerConfig listenerConfig : config.getEntryListenerConfigs()) {
            MapListener listener = getListener(listenerConfig, loader);
            if (listener != null) {
                EventFilter filter = new EntryEventFilter(null, listenerConfig.isIncludeValue());
                eventService.addListener(mapName, cacheId, listener, filter);
            }
        }
    }

    protected void setPredicateImpl(QueryCacheConfig config, ClassLoader loader) {
        PredicateConfig predicateConfig = config.getPredicateConfig();
        if (predicateConfig.getImplementation() != null) {
            return;
        }
        Predicate predicate = getPredicate(predicateConfig, loader);
        if (predicate == null) {
            return;
        }
        predicateConfig.setImplementation(predicate);
    }

    private Predicate getPredicate(PredicateConfig predicateConfig, ClassLoader loader) {

        if (!isNullOrEmpty(predicateConfig.getClassName())) {
            try {
                return ClassLoaderUtil.newInstance(loader, predicateConfig.getClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        if (!isNullOrEmpty(predicateConfig.getSql())) {
            String sql = predicateConfig.getSql();
            return Predicates.sql(sql);
        }

        return null;
    }

    private <T extends EventListener> T getListener(ListenerConfig listenerConfig, ClassLoader loader) {
        T listener = null;
        if (listenerConfig.getImplementation() != null) {
            listener = (T) listenerConfig.getImplementation();
        } else if (listenerConfig.getClassName() != null) {
            try {
                return ClassLoaderUtil.newInstance(loader, listenerConfig.getClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return listener;
    }
}
