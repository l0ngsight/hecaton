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

package uk.org.hecaton.cache;

import java.net.URI;

/**
 * Utility class for various cache related operations to be used by our internal structure and end user.
 *
 * @since 3.7
 */
public final class CacheUtil {

    private CacheUtil() {
    }

    /**
     * Gets the prefix of cache name without Hecaton's {@link javax.cache.CacheManager}
     * specific prefix {@link uk.org.hecaton.cache.HecatonCacheManager#CACHE_MANAGER_PREFIX}.
     *
     * @param uri         an implementation specific URI for the
     *                    Hecaton's {@link javax.cache.CacheManager} (null means use
     *                    Hecaton's {@link javax.cache.spi.CachingProvider#getDefaultURI()})
     * @param classLoader the {@link ClassLoader}  to use for the
     *                    Hecaton's {@link javax.cache.CacheManager} (null means use
     *                    Hecaton's {@link javax.cache.spi.CachingProvider#getDefaultClassLoader()})
     * @return the prefix of cache name without Hecaton's {@link javax.cache.CacheManager}
     * specific prefix {@link uk.org.hecaton.cache.HecatonCacheManager#CACHE_MANAGER_PREFIX}
     */
    public static String getPrefix(URI uri, ClassLoader classLoader) {
        if (uri == null && classLoader == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();
            if (uri != null) {
                sb.append(uri.toASCIIString()).append('/');
            }
            if (classLoader != null) {
                sb.append(classLoader).append('/');
            }
            return sb.toString();
        }
    }

    /**
     * Gets the cache name with prefix but without Hecaton's {@link javax.cache.CacheManager}
     * specific prefix {@link uk.org.hecaton.cache.HecatonCacheManager#CACHE_MANAGER_PREFIX}.
     *
     * @param name        the simple name of the cache without any prefix
     * @param uri         an implementation specific URI for the
     *                    Hecaton's {@link javax.cache.CacheManager} (null means use
     *                    Hecaton's {@link javax.cache.spi.CachingProvider#getDefaultURI()})
     * @param classLoader the {@link ClassLoader}  to use for the
     *                    Hecaton's {@link javax.cache.CacheManager} (null means use
     *                    Hecaton's {@link javax.cache.spi.CachingProvider#getDefaultClassLoader()})
     * @return the cache name with prefix but without Hecaton's {@link javax.cache.CacheManager}
     * specific prefix {@link uk.org.hecaton.cache.HecatonCacheManager#CACHE_MANAGER_PREFIX}.
     */
    public static String getPrefixedCacheName(String name, URI uri, ClassLoader classLoader) {
        String cacheNamePrefix = getPrefix(uri, classLoader);
        if (cacheNamePrefix != null) {
            return cacheNamePrefix + name;
        } else {
            return name;
        }
    }

    /**
     * Convenience method to obtain the name of Hecaton distributed object corresponding to the cache identified
     * by the given {@code cacheName}, assuming {@code null URI} and {@code ClassLoader} prefixes. This is equivalent to
     * invoking {@link #getDistributedObjectName(String, URI, ClassLoader)} with {@code null} passed as {@code URI} and
     * {@code ClassLoader} arguments.
     *
     * @param cacheName   the simple name of the cache without any prefix
     * @return            the name of the {@link ICache} distributed object corresponding to given cacheName, assuming
     *                    null URI &amp; class loader prefixes.
     * @see #getDistributedObjectName(String, URI, ClassLoader)
     */
    public static String getDistributedObjectName(String cacheName) {
        return getDistributedObjectName(cacheName, null, null);
    }

    /**
     * Convenience method to obtain the name of Hecaton distributed object corresponding to the cache identified
     * by the given arguments. The distributed object name returned by this method can be used to obtain the cache as
     * a Hecaton {@link ICache} as shown in this example:
     * <pre>
     *      HecatonInstance hz = Hecaton.newHecatonInstance();
     *
     *      // Obtain Cache via JSR-107 API
     *      CachingProvider hecatonCachingProvider = Caching.getCachingProvider(
     *                  "uk.org.hecaton.cache.HecatonCachingProvider",
     *                  HecatonCachingProvider.class.getClassLoader());
     *      CacheManager cacheManager = hecatonCachingProvider.getCacheManager();
     *      Cache testCache = cacheManager.createCache("test", new MutableConfiguration&lt;String, String&gt;());
     *
     *      // URI and ClassLoader are null, since we created this cache with the default CacheManager,
     *      // otherwise we should pass the owning CacheManager's URI &amp; ClassLoader as arguments.
     *      String distributedObjectName = CacheUtil.asDistributedObjectName("test", null, null);
     *
     *      // Obtain a reference to the backing ICache via HecatonInstance.getDistributedObject.
     *      // You may invoke this on any member of the cluster.
     *      ICache&lt;String, String&gt; distributedObjectCache = (ICache) hz.getDistributedObject(
     *                  ICacheService.SERVICE_NAME,
     *                  distributedObjectName);
     * </pre>
     *
     * @param cacheName   the simple name of the cache without any prefix
     * @param uri         an implementation specific URI for the
     *                    Hecaton's {@link javax.cache.CacheManager} (null means use
     *                    Hecaton's {@link javax.cache.spi.CachingProvider#getDefaultURI()})
     * @param classLoader the {@link ClassLoader}  to use for the
     *                    Hecaton's {@link javax.cache.CacheManager} (null means use
     *                    Hecaton's {@link javax.cache.spi.CachingProvider#getDefaultClassLoader()})
     * @return            the name of the {@link ICache} distributed object corresponding to given arguments.
     */
    public static String getDistributedObjectName(String cacheName, URI uri, ClassLoader classLoader) {
        return HecatonCacheManager.CACHE_MANAGER_PREFIX + getPrefixedCacheName(cacheName, uri, classLoader);
    }
}
