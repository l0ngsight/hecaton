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
package uk.org.hecaton.cache.impl.journal;

import uk.org.hecaton.cache.EventJournalCacheEvent;
import uk.org.hecaton.cache.impl.AbstractHecatonCacheManager;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.journal.AbstractEventJournalExpiringTest;
import uk.org.hecaton.journal.EventJournalTestContext;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;

import javax.cache.spi.CachingProvider;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.jet.impl.util.Util.getNodeEngine;

@Category({SlowTest.class, ParallelJVMTest.class})
public class CacheEventJournalExpiringTest<K, V> extends AbstractEventJournalExpiringTest<EventJournalCacheEvent> {

    private static final String CACHE_NAME = "cachey";
    private AbstractHecatonCacheManager cacheManager;

    @Override
    protected Config getConfig() {
        Config defConfig = super.getConfig();
        var cacheConfig = defConfig.getCacheConfig(CACHE_NAME);
        cacheConfig.setEventJournalConfig(getEventJournalConfig());
        return defConfig;
    }

    @Override
    protected EventJournalTestContext<K, V, EventJournalCacheEvent<K, V>> createContext() {
        CachingProvider cachingProvider = createServerCachingProvider(getRandomInstance());
        cacheManager = (AbstractHecatonCacheManager) cachingProvider.getCacheManager();

        return new EventJournalTestContext<>(
                new EventJournalCacheDataStructureAdapter<>(cacheManager.getCache(CACHE_NAME)),
                null,
                new EventJournalCacheEventAdapter<>()
        );
    }

    @Override
    protected String getName() {
        return CACHE_NAME;
    }

    protected RingbufferContainer<?, ?> getRingBufferContainer(String name, int partitionId, HecatonInstance instance) {
        var serviceName = RingbufferService.SERVICE_NAME;
        var fullName = cacheManager.getCacheNameWithPrefix(name);
        final RingbufferService service = getNodeEngine(instance).getService(serviceName);
        final ObjectNamespace ns = CacheService.getObjectNamespace(fullName);

        RingbufferContainer<?, ?> ringbuffer = service.getContainerOrNull(partitionId, ns);
        if (ringbuffer == null) {
            throw new IllegalStateException("No ringbuffer container for partition " + partitionId);
        }
        return ringbuffer;
    }
}
