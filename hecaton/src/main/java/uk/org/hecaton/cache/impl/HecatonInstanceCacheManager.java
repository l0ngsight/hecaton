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

package uk.org.hecaton.cache.impl;

import uk.org.hecaton.cache.HecatonCacheManager;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.ICacheManager;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.spi.exception.ServiceNotFoundException;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * Hecaton {@code ICacheManager} implementation accessible via {@code HecatonInstance} that provides access to JSR-107
 * (JCache) caches configured on the cluster via Hecaton API.
 *
 * @see uk.org.hecaton.core.ICacheManager
 */
public class HecatonInstanceCacheManager implements ICacheManager {

    private final HecatonInstanceImpl original;

    public HecatonInstanceCacheManager(HecatonInstanceImpl original) {
        this.original = original;
    }

    @Override
    public <K, V> ICache<K, V> getCache(String name) {
        checkNotNull(name, "Retrieving a cache instance with a null name is not allowed!");
        return getCacheByFullName(HecatonCacheManager.CACHE_MANAGER_PREFIX + name);
    }

    public <K, V> ICache<K, V> getCacheByFullName(String fullName) {
        checkNotNull(fullName, "Retrieving a cache instance with a null name is not allowed!");
        try {
            return original.getDistributedObject(ICacheService.SERVICE_NAME, fullName);
        } catch (HecatonException e) {
            if (e.getCause() instanceof ServiceNotFoundException) {
                throw new IllegalStateException(ICacheService.CACHE_SUPPORT_NOT_AVAILABLE_ERROR_MESSAGE);
            } else {
                throw e;
            }
        }
    }
}
