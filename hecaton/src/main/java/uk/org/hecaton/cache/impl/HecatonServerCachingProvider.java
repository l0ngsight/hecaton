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

import uk.org.hecaton.cache.HecatonCachingProvider;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigBuilder;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.config.YamlConfigBuilder;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmpty;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Hecaton server implementation of {@link javax.cache.spi.CachingProvider}.
 * <p>
 * Used internally by {@link uk.org.hecaton.cache.HecatonCachingProvider} when
 * the JCache type is configured as {@code server}.
 * <p>
 * This implementation may create a new or reuse an existing {@link HecatonInstance}
 * member.
 *
 * @see javax.cache.spi.CachingProvider
 */
public final class HecatonServerCachingProvider extends AbstractHecatonCachingProvider {

    public HecatonServerCachingProvider() {
    }

    /**
     * Creates a new {@code HecatonServerCachingProvider} with an
     * existing default fallback {@code HecatonInstance}.
     *
     * @param instance  an existing, running {@code HecatonInstance} which will be used
     *                  as default if no other means to identify a {@code HecatonInstance}
     *                  are provided when constructing a {@code CacheManager} from this
     *                  {@code CachingProvider}.
     */
    public HecatonServerCachingProvider(HecatonInstance instance) {
        this.hecatonInstance = instance;
    }

    @Override
    public String toString() {
        return "HecatonServerCachingProvider{hecatonInstance=" + hecatonInstance + '}';
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends AbstractHecatonCacheManager> T createCacheManager(HecatonInstance instance, URI uri,
                                                                             ClassLoader classLoader, Properties properties) {
        return (T) new HecatonServerCacheManager(this, instance, uri, classLoader, properties);
    }

    @Nonnull
    @Override
    protected HecatonInstance getOrCreateFromUri(@Nonnull URI uri,
                                                   ClassLoader classLoader,
                                                   String instanceName)
            throws URISyntaxException, IOException {
        Config config = getConfigFromLocation(uri, classLoader, instanceName);
        return HecatonInstanceFactory.getOrCreateHecatonInstance(config);
    }

    @Nonnull
    @Override
    protected HecatonInstance getDefaultInstance() {
        if (hecatonInstance == null) {
            // if there is no default instance in use (not created yet and not specified):
            // 1. locate default Config: if it specifies an instance name, get-or-create an instance by that name
            // 2. otherwise start a new Hecaton member
            Config config = getDefaultConfig();
            if (isNullOrEmptyAfterTrim(config.getInstanceName())) {
                hecatonInstance = Hecaton.newHecatonInstance();
            } else {
                hecatonInstance = Hecaton.getOrCreateHecatonInstance(config);
            }
        }
        return hecatonInstance;
    }

    @Override
    protected HecatonInstance getOrCreateByInstanceName(String instanceName) {
        HecatonInstance instance = Hecaton.getHecatonInstanceByName(instanceName);
        if (instance == null) {
            Config config = getDefaultConfig();
            config.setInstanceName(instanceName);
            instance = Hecaton.getOrCreateHecatonInstance(config);
        }
        return instance;
    }

    private Config getDefaultConfig() {
        Config config = new XmlConfigBuilder().build();
        if (namedDefaultHzInstance && isNullOrEmpty(config.getInstanceName())) {
            config.setInstanceName(HecatonCachingProvider.SHARED_JCACHE_INSTANCE_NAME);
        }
        return config;
    }

    private Config getConfigFromLocation(URI location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        ClassLoader classLoaderOrDefault = classLoader == null ? getDefaultClassLoader() : classLoader;
        URL configURL = getConfigURL(location, classLoaderOrDefault);
        try {
            return getConfig(configURL, classLoaderOrDefault, instanceName);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private Config getConfig(URL configURL, ClassLoader theClassLoader, String instanceName) throws IOException {
        ConfigBuilder configBuilder = isYamlConfiguration(configURL)
                ? new YamlConfigBuilder(configURL)
                : new XmlConfigBuilder(configURL);
        Config config = configBuilder.build().setClassLoader(theClassLoader);
        if (instanceName != null) {
            // if the instance name is specified via properties use it,
            // even though instance name is specified in the config
            config.setInstanceName(instanceName);
        } else if (config.getInstanceName() == null) {
            // use the config URL as instance name if instance name is not specified
            config.setInstanceName(configURL.toString());
        }
        return config;
    }
}
