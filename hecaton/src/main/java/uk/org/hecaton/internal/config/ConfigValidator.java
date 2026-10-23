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

package uk.org.hecaton.internal.config;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CollectionConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DiskTierConfig;
import uk.org.hecaton.config.EndpointConfig;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.LoginModuleConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy;
import uk.org.hecaton.config.NearCachePreloaderConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.ScheduledExecutorConfig;
import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.config.TieredStoreConfig;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.cp.CPSubsystemConfig;
import uk.org.hecaton.config.security.JaasAuthenticationConfig;
import uk.org.hecaton.config.security.RealmConfig;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.ProtocolType;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.internal.util.MutableInteger;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.eviction.EvictionPolicyComparator;
import uk.org.hecaton.spi.merge.MergingValue;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.org.hecaton.config.EvictionPolicy.LFU;
import static uk.org.hecaton.config.EvictionPolicy.LRU;
import static uk.org.hecaton.config.EvictionPolicy.NONE;
import static uk.org.hecaton.config.EvictionPolicy.RANDOM;
import static uk.org.hecaton.config.InMemoryFormat.NATIVE;
import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static uk.org.hecaton.config.MaxSizePolicy.FREE_HEAP_PERCENTAGE;
import static uk.org.hecaton.config.MaxSizePolicy.FREE_HEAP_SIZE;
import static uk.org.hecaton.config.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static uk.org.hecaton.config.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE;
import static uk.org.hecaton.config.MaxSizePolicy.PER_NODE;
import static uk.org.hecaton.config.MaxSizePolicy.PER_PARTITION;
import static uk.org.hecaton.config.MaxSizePolicy.USED_HEAP_PERCENTAGE;
import static uk.org.hecaton.config.MaxSizePolicy.USED_HEAP_SIZE;
import static uk.org.hecaton.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static uk.org.hecaton.config.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static uk.org.hecaton.instance.ProtocolType.MEMBER;
import static uk.org.hecaton.instance.ProtocolType.WAN;
import static uk.org.hecaton.internal.config.MergePolicyValidator.checkMapMergePolicy;
import static uk.org.hecaton.internal.config.MergePolicyValidator.checkMergeTypeProviderHasRequiredTypes;
import static uk.org.hecaton.internal.nio.ClassLoaderUtil.isClassAvailable;
import static uk.org.hecaton.internal.util.Preconditions.checkTrue;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;

/**
 * Validates a Hecaton configuration in a specific
 * context like OS vs. EE or client vs. member nodes.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:methodcount"})
public final class ConfigValidator {

    public static final Set<EvictionPolicy> COMMONLY_SUPPORTED_EVICTION_POLICIES = EnumSet.of(LRU, LFU);

    private static final Set<MaxSizePolicy> NEAR_CACHE_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES
            = EnumSet.of(MaxSizePolicy.ENTRY_COUNT);

    private static final Set<EvictionPolicy> MAP_SUPPORTED_EVICTION_POLICIES
            = EnumSet.of(LRU, LFU, RANDOM, NONE);

    private static final Set<MaxSizePolicy> MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES
            = EnumSet.of(PER_NODE, PER_PARTITION, USED_NATIVE_MEMORY_PERCENTAGE,
            FREE_NATIVE_MEMORY_PERCENTAGE, USED_NATIVE_MEMORY_SIZE, FREE_NATIVE_MEMORY_SIZE);

    private static final Set<MaxSizePolicy> MAP_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES
            = EnumSet.of(PER_NODE, PER_PARTITION, USED_HEAP_SIZE, USED_HEAP_PERCENTAGE,
            FREE_HEAP_SIZE, FREE_HEAP_PERCENTAGE);

    private static final Set<MaxSizePolicy> CACHE_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES
            = EnumSet.of(ENTRY_COUNT);

    private static final Set<MaxSizePolicy> CACHE_SUPPORTED_NATIVE_MAX_SIZE_POLICIES
            = EnumSet.of(USED_NATIVE_MEMORY_PERCENTAGE,
            FREE_NATIVE_MEMORY_PERCENTAGE, USED_NATIVE_MEMORY_SIZE, FREE_NATIVE_MEMORY_SIZE);

    private ConfigValidator() {
    }

    /**
     * Validates the given {@link MapConfig}.
     *
     * @param mapConfig the {@link MapConfig}
     */
    public static void checkMapConfig(Config config, MapConfig mapConfig,
                                      SplitBrainMergePolicyProvider mergePolicyProvider) {

        // the tiered store needs NATIVE: name the tiered store's module first
        checkTSEnabledOnEnterpriseJar(mapConfig.getTieredStoreConfig());
        checkNotNativeWhenOpenSource(mapConfig.getInMemoryFormat());
        checkNotBitmapIndexWhenNativeMemory(mapConfig.getInMemoryFormat(), mapConfig.getIndexConfigs());

        if (HecatonFeature.TIERED_STORE.isAvailable()) {
            checkTieredStoreMapConfig(config, mapConfig);
        }
        if (HecatonFeature.HIGH_DENSITY_MEMORY.isAvailable()) {
            checkMapNativeConfig(mapConfig, config.getNativeMemoryConfig());
        }

        checkMapEvictionConfig(mapConfig.getEvictionConfig());
        checkMapMaxSizePolicyPerInMemoryFormat(mapConfig);
        checkMapMergePolicy(mapConfig,
                mapConfig.getMergePolicyConfig().getPolicy(), mergePolicyProvider);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public static void checkTieredStoreMapConfig(Config config, MapConfig mapConfig) {
        if (!mapConfig.getTieredStoreConfig().isEnabled()) {
            return;
        }

        String mapName = mapConfig.getName();
        if (!config.getNativeMemoryConfig().isEnabled()) {
            throw new InvalidConfigurationException(String.format("Map '%s' is configured for tiered "
                    + "storage, but native memory is not enabled.", mapName));
        }
        DiskTierConfig diskTierConfig = mapConfig.getTieredStoreConfig().getDiskTierConfig();
        if (!diskTierConfig.isEnabled()) {
            throw new InvalidConfigurationException(String.format("Map '%s' is configured for tiered "
                    + "storage, but disk tier store is not enabled. Currently this is not supported, "
                    + "disk tier store must be enabled", mapName));
        }
        String deviceName = diskTierConfig.getDeviceName();
        if (config.getDeviceConfig(deviceName) == null) {
            throw new InvalidConfigurationException(String.format("Map '%s' is configured for tiered "
                    + "storage, but there is no configuration found for the device '%s' to be used for "
                    + "the map", mapName, deviceName));
        }

        if (mapConfig.getDataPersistenceConfig().isEnabled()) {
            throw new InvalidConfigurationException(String.format("Map '%s' is configured for tiered "
                    + "storage, but data persistence is also configured. Tiered store and data "
                    + "persistence are mutually exclusive features.", mapName));
        }

        InMemoryFormat inMemoryFormat = mapConfig.getInMemoryFormat();
        if (NATIVE != inMemoryFormat) {
            throw new InvalidConfigurationException(format("Only NATIVE in-memory-format "
                            + "is supported for Tiered-Store but found [%s] for the map [%s]",
                    inMemoryFormat, mapConfig.getName()));
        }

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        if (EvictionPolicy.NONE != evictionConfig.getEvictionPolicy()) {
            throw new InvalidConfigurationException(format("Eviction is not supported "
                    + "for Tiered-Store map [%s]", mapConfig.getName()));
        }

        int timeToLiveSeconds = mapConfig.getTimeToLiveSeconds();
        if (timeToLiveSeconds != MapConfig.DISABLED_TTL_SECONDS) {
            throw new InvalidConfigurationException(format("TTL expiry is not supported "
                    + "for Tiered-Store map [%s]", mapConfig.getName()));
        }

        int maxIdleSeconds = mapConfig.getMaxIdleSeconds();
        if (maxIdleSeconds != MapConfig.DEFAULT_MAX_IDLE_SECONDS) {
            throw new InvalidConfigurationException(format("MaxIdle expiry is not supported"
                    + " for Tiered-Store map [%s]", mapConfig.getName()));
        }
    }

    static void checkMapMaxSizePolicyPerInMemoryFormat(MapConfig mapConfig) {
        MaxSizePolicy maxSizePolicy = mapConfig.getEvictionConfig().getMaxSizePolicy();
        InMemoryFormat inMemoryFormat = mapConfig.getInMemoryFormat();

        if (inMemoryFormat == NATIVE) {
            if (!MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {
                throwNotMatchingMaxSizePolicy(inMemoryFormat, maxSizePolicy, MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES);
            }
        } else {
            if (!MAP_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {
                throwNotMatchingMaxSizePolicy(inMemoryFormat, maxSizePolicy, MAP_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES);
            }
        }
    }

    private static void throwNotMatchingMaxSizePolicy(InMemoryFormat inMemoryFormat,
                                                      MaxSizePolicy maxSizePolicy,
                                                      Set<MaxSizePolicy> policies) {
        String msg = "%s is not a valid max size policy to use with"
                + " in memory format %s. Please select an appropriate one from list: %s";
        throw new InvalidConfigurationException(format(msg, maxSizePolicy, inMemoryFormat, policies));
    }

    /**
     * Checks the configuration of an immutable map (load protection, F23): its values are shared
     * as stored, so its format must be {@code BINARY}, and its entries are never removed, so it can
     * have no eviction, time-to-live or max-idle.
     */
    public static void checkImmutableMapConfig(MapConfig mapConfig) {
        String mapName = mapConfig.getName();
        if (mapConfig.getInMemoryFormat() != InMemoryFormat.BINARY) {
            throw new InvalidConfigurationException(format("Immutable map '%s' must have the BINARY in-memory format,"
                    + " not %s", mapName, mapConfig.getInMemoryFormat()));
        }
        if (mapConfig.getEvictionConfig().getEvictionPolicy() != EvictionPolicy.NONE) {
            throw new InvalidConfigurationException(format("Immutable map '%s' cannot have eviction (%s): its"
                    + " entries are never removed", mapName, mapConfig.getEvictionConfig().getEvictionPolicy()));
        }
        if (mapConfig.getTimeToLiveSeconds() > 0 || mapConfig.getMaxIdleSeconds() > 0) {
            throw new InvalidConfigurationException(format("Immutable map '%s' cannot have a time-to-live or max-idle:"
                    + " its entries never expire", mapName));
        }
    }

    public static void checkMapEvictionConfig(EvictionConfig evictionConfig) {
        EvictionPolicyComparator comparator = evictionConfig.getComparator();
        String comparatorClassName = evictionConfig.getComparatorClassName();
        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();

        checkComparatorDefinedOnlyOnce(comparatorClassName, comparator);
        checkEvictionPolicyConfiguredOnlyOnce(evictionPolicy, comparatorClassName,
                comparator, MapConfig.DEFAULT_EVICTION_POLICY);

        checkMapMaxSizePolicyConfig(evictionConfig.getMaxSizePolicy());
    }

    public static void checkMapEvictionConfig(MaxSizePolicy maxSizePolicy,
                                              EvictionPolicy evictionPolicy,
                                              String comparatorClassName,
                                              Object comparator) {
        checkEvictionConfig(evictionPolicy, comparatorClassName,
                comparator, MAP_SUPPORTED_EVICTION_POLICIES);
        checkMapMaxSizePolicyConfig(maxSizePolicy);
    }

    static void checkMapMaxSizePolicyConfig(MaxSizePolicy maxSizePolicy) {
        if (!MAP_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES.contains(maxSizePolicy)
                && !MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {

            EnumSet<MaxSizePolicy> allMaxSizePolicies = EnumSet.copyOf(MAP_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES);
            allMaxSizePolicies.addAll(MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES);

            String msg = format("IMap eviction config doesn't support max size policy `%s`. "
                    + "Please select a valid one: %s.", maxSizePolicy, allMaxSizePolicies);

            throw new InvalidConfigurationException(msg);
        }
    }

    /**
     * Checks preconditions to create a map proxy.
     *
     * @param mapConfig          the mapConfig
     * @param nativeMemoryConfig the nativeMemoryConfig
     */
    private static void checkMapNativeConfig(MapConfig mapConfig, NativeMemoryConfig nativeMemoryConfig) {
        if (NATIVE != mapConfig.getInMemoryFormat()) {
            return;
        }
        checkTrue(nativeMemoryConfig.isEnabled(),
                format("Enable native memory config to use NATIVE"
                        + " in-memory-format for the map [%s]", mapConfig.getName()));
        checkMapNativeMaxSizePolicy(mapConfig);
    }

    private static void checkMapNativeMaxSizePolicy(MapConfig mapConfig) {
        MaxSizePolicy maxSizePolicy = mapConfig.getEvictionConfig().getMaxSizePolicy();
        if (!MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {
            throw new InvalidConfigurationException("Maximum size policy " + maxSizePolicy
                    + " cannot be used with NATIVE in memory format backed Map."
                    + " Supported maximum size policies are: " + MAP_SUPPORTED_NATIVE_MAX_SIZE_POLICIES);
        }
    }

    public static void warnForUsageOfDeprecatedSymmetricEncryption(Config config, ILogger logger) {
        String warn = "Symmetric encryption is deprecated and will be removed in a future version. Consider using TLS instead.";
        boolean usesAdvancedNetworkConfig = config.getAdvancedNetworkConfig().isEnabled();

        if (config.getNetworkConfig() != null
                && config.getNetworkConfig().getSymmetricEncryptionConfig() != null
                && config.getNetworkConfig().getSymmetricEncryptionConfig().isEnabled()
                && !usesAdvancedNetworkConfig) {
            logger.warning(warn);
        }

        if (config.getAdvancedNetworkConfig() != null
                && config.getAdvancedNetworkConfig().getEndpointConfigs() != null
                && usesAdvancedNetworkConfig) {
            for (EndpointConfig endpointConfig : config.getAdvancedNetworkConfig().getEndpointConfigs().values()) {
                if (endpointConfig.getSymmetricEncryptionConfig() != null
                        && endpointConfig.getSymmetricEncryptionConfig().isEnabled()) {
                    logger.warning(warn);

                    // Write error message once if more than one endpoint use symmetric encryption
                    break;
                }
            }
        }
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity",
            "checkstyle:booleanexpressioncomplexity"})
    public static void checkAdvancedNetworkConfig(Config config) {
        if (!config.getAdvancedNetworkConfig().isEnabled()) {
            return;
        }

        EnumMap<ProtocolType, MutableInteger> serverSocketsPerProtocolType = new EnumMap<>(ProtocolType.class);
        for (ProtocolType protocolType : ProtocolType.values()) {
            serverSocketsPerProtocolType.put(protocolType, new MutableInteger());
        }

        Map<EndpointQualifier, EndpointConfig> endpointConfigs = config.getAdvancedNetworkConfig().getEndpointConfigs();

        for (EndpointConfig endpointConfig : endpointConfigs.values()) {
            if (endpointConfig instanceof ServerSocketEndpointConfig) {
                serverSocketsPerProtocolType.get(endpointConfig.getProtocolType()).getAndInc();
            }
        }

        for (ProtocolType protocolType : ProtocolType.values()) {
            int serverSocketCount = serverSocketsPerProtocolType.get(protocolType).value;
            if (serverSocketCount > protocolType.getServerSocketCardinality()) {
                throw new InvalidConfigurationException(format("Protocol type %s allows definition "
                                + "of up to %d server sockets but %d were configured", protocolType,
                        protocolType.getServerSocketCardinality(), serverSocketCount));
            }
        }

        // ensure there is 1 MEMBER type server socket
        if (serverSocketsPerProtocolType.get(MEMBER).value != 1) {
            throw new InvalidConfigurationException("A member-server-socket-endpoint"
                    + " configuration is required for the cluster to form.");
        }

        // endpoint qualifiers referenced by WAN publishers must exist
        for (WanReplicationConfig wanReplicationConfig : config.getWanReplicationConfigs().values()) {
            for (WanBatchPublisherConfig wanPublisherConfig : wanReplicationConfig.getBatchPublisherConfigs()) {
                if (wanPublisherConfig.getEndpoint() != null) {
                    EndpointQualifier qualifier = EndpointQualifier.resolve(WAN, wanPublisherConfig.getEndpoint());
                    if (endpointConfigs.get(qualifier) == null) {
                        throw new InvalidConfigurationException(
                                format("WAN publisher config for cluster name '%s' requires an wan-endpoint "
                                                + "config with identifier '%s' but none was found",
                                        wanPublisherConfig.getClusterName(), wanPublisherConfig.getEndpoint()));
                    }
                }
            }
        }
    }

    /**
     * Checks preconditions to create a map proxy with Near Cache.
     *
     * @param mapName            name of the map that Near Cache will be created for
     * @param nearCacheConfig    the {@link NearCacheConfig} to be checked
     * @param nativeMemoryConfig the {@link NativeMemoryConfig} of the Hecaton instance
     * @param isClient           {@code true} if the config is for a Hecaton client, {@code false} otherwise
     */
    public static void checkNearCacheConfig(String mapName, NearCacheConfig nearCacheConfig,
                                            NativeMemoryConfig nativeMemoryConfig, boolean isClient) {
        if (!isClient) {
            // a client's NATIVE Near Cache uses hecaton-hd when the client has it, the heap otherwise
            checkNotNativeWhenOpenSource(nearCacheConfig.getInMemoryFormat());
        }
        checkLocalUpdatePolicy(mapName, nearCacheConfig.getLocalUpdatePolicy());
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
        checkOnHeapNearCacheMaxSizePolicy(nearCacheConfig);
        checkNearCacheNativeMemoryConfig(nearCacheConfig.getInMemoryFormat(),
                nativeMemoryConfig, isClient || HecatonFeature.HIGH_DENSITY_MEMORY.isAvailable());

        if (isClient && nearCacheConfig.isCacheLocalEntries()) {
            throw new InvalidConfigurationException("The Near Cache option `cache-local-entries` is not supported in "
                    + "client configurations.");
        }
        checkPreloaderConfig(nearCacheConfig, isClient);
    }

    /**
     * Checks IMap's supported Near Cache local update policy configuration.
     *
     * @param mapName           name of the map that Near Cache will be created for
     * @param localUpdatePolicy local update policy
     */
    private static void checkLocalUpdatePolicy(String mapName, LocalUpdatePolicy localUpdatePolicy) {
        if (localUpdatePolicy != INVALIDATE) {
            throw new InvalidConfigurationException(format("Wrong `local-update-policy`"
                    + " option is selected for `%s` map Near Cache."
                    + " Only `%s` option is supported but found `%s`", mapName, INVALIDATE, localUpdatePolicy));
        }
    }

    /**
     * Checks if a {@link EvictionConfig} is valid in its context.
     *
     * @param evictionConfig the {@link EvictionConfig}
     */
    public static void checkCacheEvictionConfig(EvictionConfig evictionConfig) {
        checkEvictionConfig(evictionConfig, COMMONLY_SUPPORTED_EVICTION_POLICIES);
    }

    /**
     * Checks if a {@link EvictionConfig} is valid in its context.
     *
     * @param evictionConfig the {@link EvictionConfig}
     */
    public static void checkEvictionConfig(EvictionConfig evictionConfig,
                                           Set<EvictionPolicy> supportedEvictionPolicies) {
        if (evictionConfig == null) {
            throw new InvalidConfigurationException("Eviction config cannot be null!");
        }

        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
        String comparatorClassName = evictionConfig.getComparatorClassName();
        EvictionPolicyComparator comparator = evictionConfig.getComparator();

        checkEvictionConfig(evictionPolicy, comparatorClassName,
                comparator, supportedEvictionPolicies);
    }

    private static void checkOnHeapNearCacheMaxSizePolicy(NearCacheConfig nearCacheConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat == NATIVE) {
            return;
        }

        MaxSizePolicy maxSizePolicy = nearCacheConfig.getEvictionConfig().getMaxSizePolicy();
        if (!NEAR_CACHE_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {
            throw new InvalidConfigurationException(format("Near Cache maximum size policy %s cannot be used with %s storage."
                            + " Supported maximum size policies are: %s",
                    maxSizePolicy, inMemoryFormat, NEAR_CACHE_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES));
        }
    }

    /**
     * Checks precondition to use {@link InMemoryFormat#NATIVE}.
     *
     * @param inMemoryFormat     the {@link InMemoryFormat} of the Near Cache
     * @param nativeMemoryConfig the {@link NativeMemoryConfig} of the Hecaton instance
     * @param isEnterprise       {@code true} if the Hecaton instance is EE, {@code false} otherwise
     */
    static void checkNearCacheNativeMemoryConfig(InMemoryFormat inMemoryFormat, NativeMemoryConfig nativeMemoryConfig,
                                                 boolean isEnterprise) {
        if (!isEnterprise) {
            return;
        }
        if (inMemoryFormat != NATIVE) {
            return;
        }
        if (nativeMemoryConfig != null && nativeMemoryConfig.isEnabled()) {
            return;
        }
        throw new InvalidConfigurationException("Enable native memory config to use NATIVE in-memory-format for Near Cache");
    }

    /**
     * Checks if parameters for an {@link EvictionConfig} are valid in their context.
     *
     * @param evictionPolicy      the {@link EvictionPolicy} for the {@link EvictionConfig}
     * @param comparatorClassName the comparator class name for the {@link EvictionConfig}
     * @param comparator          the comparator implementation for the {@link EvictionConfig}
     */
    public static void checkEvictionConfig(EvictionPolicy evictionPolicy,
                                           String comparatorClassName,
                                           Object comparator,
                                           Set<EvictionPolicy> supportedEvictionPolicies) {
        checkComparatorDefinedOnlyOnce(comparatorClassName, comparator);

        if (!supportedEvictionPolicies.contains(evictionPolicy)) {
            if (isNullOrEmpty(comparatorClassName) && comparator == null) {
                String msg = format("Eviction policy `%s` is not supported. Either you can provide a custom one or "
                        + "you can use a supported one: %s.", evictionPolicy, supportedEvictionPolicies);

                throw new InvalidConfigurationException(msg);
            }
        } else {
            checkEvictionPolicyConfiguredOnlyOnce(evictionPolicy, comparatorClassName,
                    comparator, EvictionConfig.DEFAULT_EVICTION_POLICY);
        }
    }

    private static void checkComparatorDefinedOnlyOnce(String comparatorClassName, Object comparator) {
        if (comparatorClassName != null && comparator != null) {
            throw new InvalidConfigurationException("Only one of the `comparator class name` and `comparator`"
                    + " can be configured in the eviction configuration!");
        }
    }

    public static void checkNearCacheEvictionConfig(EvictionPolicy evictionPolicy,
                                                    String comparatorClassName,
                                                    Object comparator) {
        checkComparatorDefinedOnlyOnce(comparatorClassName, comparator);
        checkEvictionPolicyConfiguredOnlyOnce(evictionPolicy, comparatorClassName,
                comparator, EvictionConfig.DEFAULT_EVICTION_POLICY);
    }

    private static void checkEvictionPolicyConfiguredOnlyOnce(EvictionPolicy evictionPolicy,
                                                              String comparatorClassName,
                                                              Object comparator, EvictionPolicy defaultEvictionPolicy) {
        if (evictionPolicy != defaultEvictionPolicy) {
            if (!isNullOrEmpty(comparatorClassName)) {
                throw new InvalidConfigurationException(
                        "Only one of the `eviction policy` and `comparator class name` can be configured!");
            }
            if (comparator != null) {
                throw new InvalidConfigurationException("Only one of the `eviction policy` and `comparator` can be configured!");
            }
        }
    }

    /**
     * Validates the given {@link CacheSimpleConfig}.
     *
     * @param cacheSimpleConfig   the {@link CacheSimpleConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCacheConfig(CacheSimpleConfig cacheSimpleConfig,
                                        SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkCacheConfig(
                cacheSimpleConfig.getInMemoryFormat(),
                cacheSimpleConfig.getEvictionConfig(),
                cacheSimpleConfig.getMergePolicyConfig().getPolicy(),
                SplitBrainMergeTypes.CacheMergeTypes.class, mergePolicyProvider,
                COMMONLY_SUPPORTED_EVICTION_POLICIES,
                cacheSimpleConfig.getUserCodeNamespace());
    }

    /**
     * Validates the given {@link CacheConfig}.
     *
     * @param cacheConfig the {@link CacheConfig}
     *                    to check @param mergePolicyProvider the {@link
     *                    SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCacheConfig(
            CacheConfig cacheConfig,
            SplitBrainMergePolicyProvider mergePolicyProvider
    ) {
        checkCacheConfig(
                cacheConfig.getInMemoryFormat(),
                cacheConfig.getEvictionConfig(),
                cacheConfig.getMergePolicyConfig().getPolicy(),
                SplitBrainMergeTypes.CacheMergeTypes.class,
                mergePolicyProvider,
                COMMONLY_SUPPORTED_EVICTION_POLICIES,
                cacheConfig.getUserCodeNamespace()
        );
    }

    /**
     * Validates the given parameters in the context of an {@link ICache} config.
     * According to JSR-107, {@code javax.cache.CacheManager#createCache(String, Configuration)}
     * should throw {@link IllegalArgumentException} in case of invalid configuration.
     * Any {@link InvalidConfigurationException}s thrown from common validation methods
     * are translated to {@link IllegalArgumentException} by this method.
     *
     * @param inMemoryFormat       the {@link InMemoryFormat} of the cache
     * @param evictionConfig       the {@link EvictionConfig} of the cache
     * @param mergePolicyClassname the configured merge policy of the cache
     * @param mergeTypes           the cache merge types
     * @param mergePolicyProvider  the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    private static void checkCacheConfig(InMemoryFormat inMemoryFormat,
                                  EvictionConfig evictionConfig,
                                  String mergePolicyClassname,
                                  Class<? extends MergingValue> mergeTypes,
                                  SplitBrainMergePolicyProvider mergePolicyProvider,
                                  Set<EvictionPolicy> supportedEvictionPolicies,
                                  @Nullable String namespace) {
        try {
            checkNotNativeWhenOpenSource(inMemoryFormat);
            checkEvictionConfig(evictionConfig, supportedEvictionPolicies);
            checkCacheMaxSizePolicy(evictionConfig.getMaxSizePolicy(), inMemoryFormat);
            checkMergeTypeProviderHasRequiredTypes(mergeTypes, mergePolicyProvider, mergePolicyClassname, namespace);
        } catch (InvalidConfigurationException e) {
            throw new IllegalArgumentException(e.getMessage(), e.getCause());
        }
    }

    // package private for testing.
    static void checkCacheMaxSizePolicy(MaxSizePolicy maxSizePolicy,
                                        InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat == NATIVE) {
            if (!CACHE_SUPPORTED_NATIVE_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {
                throw new IllegalArgumentException("Maximum size policy " + maxSizePolicy
                        + " cannot be used with NATIVE in memory format backed Cache."
                        + " Supported maximum size policies are: " + CACHE_SUPPORTED_NATIVE_MAX_SIZE_POLICIES);
            }
        } else {
            if (!CACHE_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {
                String msg = format("Cache eviction config doesn't support max size policy `%s`. "
                        + "Please select a valid one: %s.", maxSizePolicy, CACHE_SUPPORTED_ON_HEAP_MAX_SIZE_POLICIES);
                throw new IllegalArgumentException(msg);
            }
        }
    }


    /**
     * Validates the given {@link ReplicatedMapConfig}.
     *
     * @param replicatedMapConfig the {@link ReplicatedMapConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider}
     *                            to resolve merge policy classes
     */
    public static void checkReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig,
                                                SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergeTypeProviderHasRequiredTypes(
                SplitBrainMergeTypes.ReplicatedMapMergeTypes.class,
                mergePolicyProvider,
                replicatedMapConfig.getMergePolicyConfig().getPolicy(),
                replicatedMapConfig.getUserCodeNamespace()
        );
    }

    /**
     * Validates the given {@link MultiMapConfig}.
     *
     * @param multiMapConfig      the {@link MultiMapConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkMultiMapConfig(MultiMapConfig multiMapConfig,
                                           SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergeTypeProviderHasRequiredTypes(
                SplitBrainMergeTypes.MultiMapMergeTypes.class,
                mergePolicyProvider,
                multiMapConfig.getMergePolicyConfig().getPolicy(),
                multiMapConfig.getUserCodeNamespace()
        );
    }

    /**
     * Validates the given {@link QueueConfig}.
     *
     * @param queueConfig         the {@link QueueConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkQueueConfig(QueueConfig queueConfig,
                                        SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergeTypeProviderHasRequiredTypes(
                SplitBrainMergeTypes.QueueMergeTypes.class,
                mergePolicyProvider,
                queueConfig.getMergePolicyConfig().getPolicy(),
                queueConfig.getUserCodeNamespace()
        );
    }

    /**
     * Validates the given {@link CollectionConfig}.
     *
     * @param collectionConfig    the {@link CollectionConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCollectionConfig(CollectionConfig collectionConfig,
                                             SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergeTypeProviderHasRequiredTypes(
                SplitBrainMergeTypes.CollectionMergeTypes.class,
                mergePolicyProvider,
                collectionConfig.getMergePolicyConfig().getPolicy(),
                collectionConfig.getUserCodeNamespace()
        );
    }

    /**
     * Validates the given {@link RingbufferConfig}.
     *
     * @param ringbufferConfig    the {@link RingbufferConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkRingbufferConfig(RingbufferConfig ringbufferConfig,
                                             SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergeTypeProviderHasRequiredTypes(
                SplitBrainMergeTypes.RingbufferMergeTypes.class,
                mergePolicyProvider,
                ringbufferConfig.getMergePolicyConfig().getPolicy(),
                ringbufferConfig.getUserCodeNamespace()
        );
    }

    /**
     * Validates the given {@link ScheduledExecutorConfig}.
     *
     * @param scheduledExecutorConfig the {@link ScheduledExecutorConfig} to check
     * @param mergePolicyProvider     the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig,
                                                    SplitBrainMergePolicyProvider mergePolicyProvider) {
        String mergePolicyClassName = scheduledExecutorConfig.getMergePolicyConfig().getPolicy();
        checkMergeTypeProviderHasRequiredTypes(
                SplitBrainMergeTypes.ScheduledExecutorMergeTypes.class,
                mergePolicyProvider,
                mergePolicyClassName,
                scheduledExecutorConfig.getUserCodeNamespace()
        );
    }

    public static void checkCPSubsystemConfig(CPSubsystemConfig config) {
        checkTrue(config.getGroupSize() <= config.getCPMemberCount(),
                "The group size parameter cannot be bigger than the number of the CP member count");

        checkTrue(config.getSessionTimeToLiveSeconds() > config.getSessionHeartbeatIntervalSeconds(),
                "Session TTL must be greater than session heartbeat interval!");

        checkTrue(config.getMissingCPMemberAutoRemovalSeconds() == 0
                        || config.getSessionTimeToLiveSeconds() <= config.getMissingCPMemberAutoRemovalSeconds(),
                "Session TTL must be smaller than or equal to missing CP member auto-removal seconds!");

        checkTrue(!config.isPersistenceEnabled() || config.getCPMemberCount() > 0,
                "CP member count must be greater than 0 to use CP persistence feature!");
    }

    /**
     * Throws {@link InvalidConfigurationException} if the given {@link InMemoryFormat}
     * is {@link InMemoryFormat#NATIVE} and Hecaton is OS.
     *
     * @param inMemoryFormat supplied inMemoryFormat
     */
    private static void checkNotNativeWhenOpenSource(InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat == NATIVE && !HecatonFeature.HIGH_DENSITY_MEMORY.isAvailable()) {
            throw new InvalidConfigurationException(HecatonFeature.HIGH_DENSITY_MEMORY.notAvailableMessage());
        }
    }

    /**
     * Throws {@link InvalidConfigurationException} if the given {@link TieredStoreConfig}
     * is enabled and Hecaton is OS.
     *
     * @param tieredStoreConfig supplied tieredStoreConfig
     */
    private static void checkTSEnabledOnEnterpriseJar(TieredStoreConfig tieredStoreConfig) {
        if (tieredStoreConfig.isEnabled() && !HecatonFeature.TIERED_STORE.isAvailable()) {
            throw new InvalidConfigurationException(HecatonFeature.TIERED_STORE.notAvailableMessage());
        }
    }

    /**
     * Throws {@link InvalidConfigurationException} if the given {@link InMemoryFormat}
     * is {@link InMemoryFormat#NATIVE} and index configurations include {@link IndexType#BITMAP}.
     *
     * @param inMemoryFormat supplied inMemoryFormat
     * @param indexConfigs   {@link List} of {@link IndexConfig}
     */
    private static void checkNotBitmapIndexWhenNativeMemory(InMemoryFormat inMemoryFormat, List<IndexConfig> indexConfigs) {
        if (inMemoryFormat == NATIVE) {
            for (IndexConfig indexConfig : indexConfigs) {
                if (indexConfig.getType() == IndexType.BITMAP) {
                    throw new InvalidConfigurationException("BITMAP indexes are not supported by NATIVE storage");
                }
            }
        }
    }

    /**
     * Throws {@link InvalidConfigurationException} if the given {@link NearCacheConfig}
     * has an invalid {@link NearCachePreloaderConfig}.
     *
     * @param nearCacheConfig supplied NearCacheConfig
     * @param isClient        {@code true} if the config is for a Hecaton client, {@code false} otherwise
     */
    private static void checkPreloaderConfig(NearCacheConfig nearCacheConfig, boolean isClient) {
        if (!isClient && nearCacheConfig.getPreloaderConfig().isEnabled()) {
            throw new InvalidConfigurationException("The Near Cache pre-loader is just available on Hecaton clients!");
        }
    }

    public static void checkSecurityConfig(Node node, SecurityConfig securityConfig) {
        if (!securityConfig.isEnabled()) {
            return;
        }
        Map<String, RealmConfig> realmConfigs = securityConfig.getRealmConfigs();
        List<String> errors = new ArrayList<>();
        if (realmConfigs != null) {
            realmConfigs.values().forEach(realmConfig -> {
                JaasAuthenticationConfig jaas = realmConfig.getJaasAuthenticationConfig();
                if (jaas != null) {
                    for (LoginModuleConfig jaasLoginModuleConfig : jaas.getLoginModuleConfigs()) {
                        ClassLoader configClassLoader = node.getConfigClassLoader();
                        String className = jaasLoginModuleConfig.getClassName();
                        if (isNotBlank(className) && !isClassAvailable(configClassLoader, className)) {
                            errors.add(className);
                        }
                    }
                }
            });
        }
        if (!errors.isEmpty()) {
            throw new InvalidConfigurationException("Login module class(es) not found: " + String.join(", ", errors));
        }
    }

    private static boolean isNotBlank(String className) {
        return className != null && !className.isEmpty();
    }
}
