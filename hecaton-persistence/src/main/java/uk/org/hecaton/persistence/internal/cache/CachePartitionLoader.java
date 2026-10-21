/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.persistence.internal.cache;

import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.spi.impl.NodeEngine;

/**
 * Loads the persisted records of one partition into the record stores of their caches, creating
 * the caches' configuration from the persisted one if the member does not know it.
 */
public final class CachePartitionLoader {

    private final ICacheService cacheService;
    private final SerializationService serializationService;
    private final int partitionId;

    public CachePartitionLoader(NodeEngine nodeEngine, int partitionId) {
        this.cacheService = nodeEngine.getService(ICacheService.SERVICE_NAME);
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionId = partitionId;
    }

    /**
     * Puts a persisted record into its record store.
     *
     * @param config the persisted configuration of the cache, or null
     */
    public void load(String cacheName, byte[] config, byte[] key, byte[] value) {
        if (cacheService.getCacheConfig(cacheName) == null) {
            if (config == null) {
                throw new IllegalStateException("The configuration of cache " + cacheName + " is missing");
            }
            cacheService.putCacheConfigIfAbsent(serializationService.<CacheConfig>toObject(new HeapData(config)));
        }
        CacheRecord<?, ?> record = serializationService.toObject(new HeapData(value));
        cacheService.getOrCreateRecordStore(cacheName, partitionId).putRecord(new HeapData(key), record, false);
    }
}
