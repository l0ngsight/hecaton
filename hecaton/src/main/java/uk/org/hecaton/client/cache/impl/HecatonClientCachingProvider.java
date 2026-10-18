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

package uk.org.hecaton.client.cache.impl;

import uk.org.hecaton.cache.HecatonCachingProvider;
import uk.org.hecaton.cache.impl.AbstractHecatonCacheManager;
import uk.org.hecaton.cache.impl.AbstractHecatonCachingProvider;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.client.config.YamlClientConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;

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
 * Hecaton client implementation of {@link javax.cache.spi.CachingProvider}.
 * <p>
 * Used internally by {@link uk.org.hecaton.cache.HecatonCachingProvider} when
 * the JCache type is configured as {@code client}.
 * <p>
 * This implementation may create a new or reuse an existing {@link HecatonInstance}
 * client.
 *
 * @see javax.cache.spi.CachingProvider
 */
public final class HecatonClientCachingProvider extends AbstractHecatonCachingProvider {

    public HecatonClientCachingProvider() {
    }

    public HecatonClientCachingProvider(HecatonInstance instance) {
        this.hecatonInstance = instance;
    }

    @Override
    public String toString() {
        return "HecatonClientCachingProvider{hecatonInstance=" + hecatonInstance + '}';
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends AbstractHecatonCacheManager> T createCacheManager(HecatonInstance instance, URI uri,
                                                                             ClassLoader classLoader, Properties properties) {
        return (T) new HecatonClientCacheManager(this, instance, uri, classLoader, properties);
    }

    @Nonnull
    @Override
    protected HecatonInstance getOrCreateFromUri(@Nonnull URI uri,
                                                            ClassLoader classLoader,
                                                            String instanceName)
            throws URISyntaxException, IOException {
        ClientConfig config = getConfigFromLocation(uri, classLoader, instanceName);
        return getOrCreateInstanceByConfig(config);
    }

    @Nonnull
    @Override
    protected HecatonInstance getDefaultInstance() {
        if (hecatonInstance == null) {
            // if there is no default instance in use (not created yet and not specified):
            // 1. locate default ClientConfig: if it specifies an instance name, get-or-create an instance by that name
            // 2. otherwise start a new Hecaton client
            ClientConfig clientConfig = getDefaultClientConfig();
            if (isNullOrEmptyAfterTrim(clientConfig.getInstanceName())) {
                hecatonInstance = HecatonClient.newHecatonClient();
            } else {
                hecatonInstance = getOrCreateInstanceByConfig(clientConfig);
            }
        }
        return hecatonInstance;
    }

    @Override
    protected HecatonInstance getOrCreateByInstanceName(String instanceName) {
        HecatonInstance instance = HecatonClient.getHecatonClientByName(instanceName);
        if (instance == null) {
            ClientConfig clientConfig = getDefaultClientConfig();
            clientConfig.setInstanceName(instanceName);
            instance = HecatonClient.newHecatonClient(clientConfig);
        }
        return instance;
    }

    private ClientConfig getDefaultClientConfig() {
        ClientConfig clientConfig = new XmlClientConfigBuilder().build();
        if (namedDefaultHzInstance && isNullOrEmpty(clientConfig.getInstanceName())) {
            clientConfig.setInstanceName(HecatonCachingProvider.SHARED_JCACHE_INSTANCE_NAME);
        }
        return clientConfig;
    }

    private ClientConfig getConfigFromLocation(URI location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        ClassLoader classLoaderOrDefault = classLoader == null ? getDefaultClassLoader() : classLoader;
        URL configURL = getConfigURL(location, classLoaderOrDefault);
        try {
            return getConfig(configURL, classLoaderOrDefault, instanceName);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private ClientConfig getConfig(URL configURL, ClassLoader theClassLoader, String instanceName) throws IOException {
        ClientConfig config = isYamlConfiguration(configURL)
                ? new YamlClientConfigBuilder(configURL).build()
                : new XmlClientConfigBuilder(configURL).build();
        config.setClassLoader(theClassLoader);
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

    private HecatonInstance getOrCreateInstanceByConfig(ClientConfig config) {
        // lookup by config.getInstanceName(), if not found return a new HecatonInstance
        HecatonInstance instance = HecatonClient.getHecatonClientByName(config.getInstanceName());
        if (instance == null) {
            instance = HecatonClient.newHecatonClient(config);
        }
        return instance;
    }
}
