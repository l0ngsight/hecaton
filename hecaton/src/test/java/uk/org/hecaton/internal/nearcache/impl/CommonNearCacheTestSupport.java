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

import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.internal.nearcache.NearCacheRecordStore;
import uk.org.hecaton.internal.nearcache.impl.store.NearCacheDataRecordStore;
import uk.org.hecaton.internal.nearcache.impl.store.NearCacheObjectRecordStore;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.spi.impl.executionservice.TaskScheduler;
import uk.org.hecaton.spi.impl.executionservice.impl.DelegatingTaskScheduler;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class CommonNearCacheTestSupport extends HecatonTestSupport {

    static final int DEFAULT_RECORD_COUNT = 100;
    static final String DEFAULT_NEAR_CACHE_NAME = "TestNearCache";

    private List<ScheduledExecutorService> scheduledExecutorServices = new ArrayList<>();
    private SerializationService ss = new DefaultSerializationServiceBuilder()
            .setVersion(InternalSerializationService.VERSION_1).build();

    @After
    public final void shutdownExecutorServices() {
        for (ScheduledExecutorService scheduledExecutorService : scheduledExecutorServices) {
            scheduledExecutorService.shutdown();
        }
        scheduledExecutorServices.clear();
    }

    NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setName(name)
                .setInMemoryFormat(inMemoryFormat);
    }

    <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig, InMemoryFormat inMemoryFormat) {
        NearCacheRecordStore<K, V> recordStore;
        HecatonProperties emptyProperties = new HecatonProperties(new Properties());
        switch (inMemoryFormat) {
            case BINARY:
                recordStore = new NearCacheDataRecordStore<>(DEFAULT_NEAR_CACHE_NAME, nearCacheConfig, ss, null, emptyProperties);
                break;
            case OBJECT:
                recordStore = new NearCacheObjectRecordStore<>(DEFAULT_NEAR_CACHE_NAME, nearCacheConfig, ss, null, emptyProperties);
                break;
            default:
                throw new IllegalArgumentException("Unsupported in-memory format: " + inMemoryFormat);
        }
        recordStore.initialize();
        return recordStore;
    }

    @SuppressWarnings("unused")
    TaskScheduler createTaskScheduler() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorServices.add(scheduledExecutorService);
        return new DelegatingTaskScheduler(scheduledExecutorService, scheduledExecutorService);
    }
}
