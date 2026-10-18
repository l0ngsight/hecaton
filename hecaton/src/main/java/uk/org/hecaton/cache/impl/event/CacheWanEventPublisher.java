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

package uk.org.hecaton.cache.impl.event;

import uk.org.hecaton.cache.CacheEntryView;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.Operation;

/**
 * This interface provides methods to publish wan replication events
 * from cache operations.
 * <p>
 * Methods of this class should be called from within the partition thread
 * to keep event order for keys belonging to a single partition.
 */
public interface CacheWanEventPublisher {

    /**
     * This method will create a wrapper object using the given {@link CacheEntryView}
     * and place it to wan replication queues.
     * <p>
     * Updating cache operations should call this method in their {@link Operation#afterRun()} method.
     *
     * @param cacheNameWithPrefix the full name of the {@link uk.org.hecaton.cache.ICache}, including the manager scope prefix
     * @param entryView           the updated cache entry
     * @see uk.org.hecaton.cache.impl.operation.CachePutOperation
     * @see uk.org.hecaton.cache.impl.operation.CacheGetAndReplaceOperation
     */
    void publishWanUpdate(String cacheNameWithPrefix, CacheEntryView<Data, Data> entryView);

    /**
     * This method will create a wrapper object using the given {@link CacheEntryView}
     * and place it to wan replication queues.
     * <p>
     * Cache operations which removes data from cache should call this method in their
     * {@link Operation#afterRun()} method.
     *
     * @param cacheNameWithPrefix the full name of the {@link uk.org.hecaton.cache.ICache}, including the manager scope prefix
     * @param key                 the key of the removed entry
     * @see uk.org.hecaton.cache.impl.operation.CacheRemoveOperation
     */
    void publishWanRemove(String cacheNameWithPrefix, Data key);
}
