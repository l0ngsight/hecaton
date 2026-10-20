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

package uk.org.hecaton.spring.config;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientFailoverConfig;
import uk.org.hecaton.config.AliasedDiscoveryConfig;
import uk.org.hecaton.config.CompactSerializationConfig;
import uk.org.hecaton.config.CompactSerializationConfigAccessor;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.internal.config.AliasedDiscoveryConfigUtils;
import uk.org.hecaton.spi.eviction.EvictionPolicyComparator;
import uk.org.hecaton.spring.HecatonClientBeanDefinitionParser;
import uk.org.hecaton.spring.HecatonConfigBeanDefinitionParser;
import uk.org.hecaton.spring.HecatonFailoverClientBeanDefinitionParser;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static uk.org.hecaton.internal.config.ConfigValidator.COMMONLY_SUPPORTED_EVICTION_POLICIES;
import static uk.org.hecaton.internal.config.ConfigValidator.checkEvictionConfig;
import static uk.org.hecaton.internal.config.ConfigValidator.checkMapEvictionConfig;
import static uk.org.hecaton.internal.config.ConfigValidator.checkNearCacheEvictionConfig;

/**
 * Provides factory methods for {@link Config} and {@link ClientConfig}.
 * This class is used in {@link HecatonConfigBeanDefinitionParser},
 * {@link HecatonClientBeanDefinitionParser} and
 * {@link HecatonFailoverClientBeanDefinitionParser} to create
 * `empty` configuration instances. This factory can be used to
 * pre-configure the configuration instances, see
 * {@code CustomSpringJUnit4ClassRunner} for the usage.
 */
public final class ConfigFactory {

    private static volatile Supplier<Config> configSupplier = Config::new;
    private static volatile Supplier<ClientConfig> clientConfigSupplier = ClientConfig::new;
    private static volatile Supplier<ClientFailoverConfig> clientFailoverConfigSupplier = ClientFailoverConfig::new;

    private ConfigFactory() {
    }

    static void setConfigSupplier(Supplier<Config> configSupplier) {
        ConfigFactory.configSupplier = configSupplier;
    }

    static void setClientConfigSupplier(Supplier<ClientConfig> clientConfigSupplier) {
        ConfigFactory.clientConfigSupplier = clientConfigSupplier;
    }

    static void setClientFailoverConfigSupplier(Supplier<ClientFailoverConfig> clientFailoverConfigSupplier) {
        ConfigFactory.clientFailoverConfigSupplier = clientFailoverConfigSupplier;
    }

    public static Config newConfig() {
        return configSupplier.get();
    }

    public static ClientConfig newClientConfig() {
        return clientConfigSupplier.get();
    }

    public static ClientFailoverConfig newClientFailoverConfig() {
        return clientFailoverConfigSupplier.get();
    }

    public static EvictionConfig newEvictionConfig(Integer maxSize, MaxSizePolicy maxSizePolicy, EvictionPolicy evictionPolicy,
                                                   boolean isNearCache, boolean isIMap, String comparatorClassName,
                                                   EvictionPolicyComparator<?, ?, ?> comparator) {
        int finalSize = maxSize(maxSize, isIMap);
        MaxSizePolicy finalMaxSizePolicy = maxSizePolicy(maxSizePolicy, isIMap);
        EvictionPolicy finalEvictionPolicy = evictionPolicy(evictionPolicy, isIMap);

        try {
            doEvictionConfigChecks(finalMaxSizePolicy, finalEvictionPolicy,
                    comparatorClassName, comparator, isIMap, isNearCache);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException(e.getMessage());
        }

        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(finalSize)
                .setMaxSizePolicy(finalMaxSizePolicy)
                .setEvictionPolicy(finalEvictionPolicy);

        if (comparatorClassName != null) {
            evictionConfig.setComparatorClassName(comparatorClassName);
        }
        if (comparator != null) {
            evictionConfig.setComparator(comparator);
        }
        return evictionConfig;
    }

    @SuppressWarnings("rawtypes")
    public static AliasedDiscoveryConfig newAliasedDiscoveryConfig(String tag, Map<String, String> properties) {
        AliasedDiscoveryConfig config = AliasedDiscoveryConfigUtils.newConfigFor(tag);
        properties.forEach(config::setProperty);
        return config;
    }

    public static CompactSerializationConfig newCompactSerializationConfig(
            List<String> serializerClassNames,
            List<String> compactSerializableClassNames
    ) {
        CompactSerializationConfig config = new CompactSerializationConfig();

        for (String compactSerializableClassName : compactSerializableClassNames) {
            CompactSerializationConfigAccessor.registerClass(config, compactSerializableClassName);
        }

        for (String serializerClassName : serializerClassNames) {
            CompactSerializationConfigAccessor.registerSerializer(config, serializerClassName);
        }

        return config;
    }

    private static int maxSize(Integer size, boolean isIMap) {
        if (size == null) {
            return isIMap ? MapConfig.DEFAULT_MAX_SIZE : EvictionConfig.DEFAULT_MAX_ENTRY_COUNT;
        }

        if (isIMap && size == 0) {
            return MapConfig.DEFAULT_MAX_SIZE;
        }
        return size;
    }

    private static EvictionPolicy evictionPolicy(EvictionPolicy evictionPolicy, boolean isIMap) {
        if (evictionPolicy == null) {
            return isIMap
                    ? MapConfig.DEFAULT_EVICTION_POLICY : EvictionConfig.DEFAULT_EVICTION_POLICY;
        }
        return evictionPolicy;
    }

    private static MaxSizePolicy maxSizePolicy(MaxSizePolicy maxSizePolicy, boolean isIMap) {
        if (maxSizePolicy == null) {
            return isIMap
                    ? MapConfig.DEFAULT_MAX_SIZE_POLICY : EvictionConfig.DEFAULT_MAX_SIZE_POLICY;
        }
        return maxSizePolicy;
    }

    private static void doEvictionConfigChecks(MaxSizePolicy maxSizePolicyValue,
                                               EvictionPolicy evictionPolicyValue,
                                               String comparatorClassNameValue,
                                               Object comparatorBeanValue,
                                               boolean isIMap, boolean isNearCache) {
        if (isIMap) {
            checkMapEvictionConfig(maxSizePolicyValue, evictionPolicyValue,
                    comparatorClassNameValue, comparatorBeanValue);
            return;
        }

        if (isNearCache) {
            checkNearCacheEvictionConfig(evictionPolicyValue,
                    comparatorClassNameValue, comparatorBeanValue);
            return;
        }

        checkEvictionConfig(evictionPolicyValue,
                comparatorClassNameValue, comparatorBeanValue,
                COMMONLY_SUPPORTED_EVICTION_POLICIES);
    }

}
