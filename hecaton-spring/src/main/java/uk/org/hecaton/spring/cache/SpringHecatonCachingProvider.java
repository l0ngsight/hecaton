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

package uk.org.hecaton.spring.cache;

import uk.org.hecaton.cache.HecatonCachingProvider;
import uk.org.hecaton.cache.impl.HecatonServerCachingProvider;
import uk.org.hecaton.client.cache.impl.HecatonClientCachingProvider;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;

import javax.cache.CacheManager;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * Spring utility class for connecting {@link HecatonCachingProvider} interface and Hecaton instance.
 */
public final class SpringHecatonCachingProvider {

    private SpringHecatonCachingProvider() {
    }

    /**
     * Creates a {@link CacheManager} on an existing HecatonInstance.
     *
     * @param uriString Scope of {@link CacheManager}
     * @param instance  Hecaton instance that created {@link CacheManager} is connected.
     * @param props     Extra properties to be passed to cache manager. If {@code props} contain hecaton.instance.name
     *                  it overrides {@code instance} parameter
     * @return the created {@link CacheManager}
     */
    public static CacheManager getCacheManager(HecatonInstance instance, String uriString, Properties props) {
        URI uri = null;
        if (uriString != null) {
            try {
                uri = new URI(uriString);
            } catch (URISyntaxException e) {
                throw rethrow(e);
            }
        }
        if (instance instanceof HecatonClientProxy) {
            return new HecatonClientCachingProvider(instance).getCacheManager(uri, null, props);
        } else {
            return new HecatonServerCachingProvider(instance).getCacheManager(uri, null, props);
        }
    }

    public static CacheManager getCacheManager(String uriString, Properties properties) {
        String instanceName = properties.getProperty(HecatonCachingProvider.HECATON_INSTANCE_NAME);
        if (instanceName == null) {
            throw new IllegalStateException("Either 'instance-ref' attribute or "
                    + HecatonCachingProvider.HECATON_INSTANCE_NAME + " property is required for creating cache manager");
        }
        return getCacheManager(Hecaton.getHecatonInstanceByName(instanceName), uriString, properties);
    }
}
