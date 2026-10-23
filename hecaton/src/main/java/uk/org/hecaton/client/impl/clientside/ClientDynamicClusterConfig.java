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

package uk.org.hecaton.client.impl.clientside;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddCacheConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddCardinalityEstimatorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddDataConnectionConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddDurableExecutorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddExecutorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddFlakeIdGeneratorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddListConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddPNCounterConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddQueueConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddReliableTopicConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddReplicatedMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddRingbufferConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddScheduledExecutorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddSetConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddTopicConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddVectorCollectionConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddWanReplicationConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder;
import uk.org.hecaton.client.impl.protocol.codec.holder.WanConsumerConfigHolder;
import uk.org.hecaton.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.QueryCacheConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.QueueStoreConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.WanReplicationConfigTransformer;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.config.AdvancedNetworkConfig;
import uk.org.hecaton.config.AuditlogConfig;
import uk.org.hecaton.config.CRDTReplicationConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CardinalityEstimatorConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigPatternMatcher;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.config.DataConnectionConfigValidator;
import uk.org.hecaton.config.DeviceConfig;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.config.DynamicConfigurationConfig;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.config.FlakeIdGeneratorConfig;
import uk.org.hecaton.config.HotRestartPersistenceConfig;
import uk.org.hecaton.config.InstanceTrackingConfig;
import uk.org.hecaton.config.IntegrityCheckerConfig;
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.LoadProtectionConfig;
import uk.org.hecaton.config.ManagementCenterConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MemberAttributeConfig;
import uk.org.hecaton.config.MetricsConfig;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.config.PNCounterConfig;
import uk.org.hecaton.config.PartitionGroupConfig;
import uk.org.hecaton.config.PersistenceConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.config.ReliableTopicConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.RingbufferStoreConfig;
import uk.org.hecaton.config.ScheduledExecutorConfig;
import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.config.SetConfig;
import uk.org.hecaton.config.SplitBrainProtectionConfig;
import uk.org.hecaton.config.SqlConfig;
import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.config.UserCodeDeploymentConfig;
import uk.org.hecaton.config.UserCodeNamespacesConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.cp.CPSubsystemConfig;
import uk.org.hecaton.config.rest.RestConfig;
import uk.org.hecaton.config.tpc.TpcConfig;
import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.core.ManagedContext;
import uk.org.hecaton.internal.config.DataPersistenceAndHotRestartMerger;
import uk.org.hecaton.internal.config.ServicesConfig;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.jet.config.JetConfig;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static uk.org.hecaton.client.impl.protocol.util.PropertiesUtil.toMap;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * Client implementation of member side config. Clients use this to submit new data structure configurations into a live
 * Hecaton cluster.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class ClientDynamicClusterConfig extends Config {
    protected static final String UNSUPPORTED_ERROR_MESSAGE =
            "Client config object only supports adding new data structure configurations";

    private final HecatonClientInstanceImpl instance;
    private final SerializationService serializationService;

    public ClientDynamicClusterConfig(HecatonClientInstanceImpl instance) {
        this.instance = instance;
        this.serializationService = instance.getSerializationService();
        userCodeNamespacesConfig = new ClientDynamicClusterUserCodeNamespacesConfig(this);
    }

    @Override
    public Config addMapConfig(MapConfig mapConfig) {
        List<ListenerConfigHolder> listenerConfigs = adaptListenerConfigs(mapConfig.getEntryListenerConfigs());
        List<ListenerConfigHolder> partitionLostListenerConfigs =
                adaptListenerConfigs(mapConfig.getPartitionLostListenerConfigs());
        List<QueryCacheConfigHolder> queryCacheConfigHolders = null;
        if (mapConfig.getQueryCacheConfigs() != null && !mapConfig.getQueryCacheConfigs().isEmpty()) {
            queryCacheConfigHolders = new ArrayList<>(mapConfig.getQueryCacheConfigs().size());
            for (QueryCacheConfig config : mapConfig.getQueryCacheConfigs()) {
                queryCacheConfigHolders.add(QueryCacheConfigHolder.of(config, serializationService));
            }
        }

        String partitioningStrategyClassName = mapConfig.getPartitioningStrategyConfig() == null ? null
                : mapConfig.getPartitioningStrategyConfig().getPartitioningStrategyClass();
        Data partitioningStrategy = mapConfig.getPartitioningStrategyConfig() == null ? null
                : serializationService.toData(mapConfig.getPartitioningStrategyConfig().getPartitioningStrategy());

        DataPersistenceAndHotRestartMerger.merge(mapConfig.getHotRestartConfig(), mapConfig.getDataPersistenceConfig());

        ClientMessage request = DynamicConfigAddMapConfigCodec.encodeRequest(mapConfig.getName(), mapConfig.getBackupCount(),
                mapConfig.getAsyncBackupCount(), mapConfig.getTimeToLiveSeconds(), mapConfig.getMaxIdleSeconds(),
                EvictionConfigHolder.of(mapConfig.getEvictionConfig(), serializationService), mapConfig.isReadBackupData(),
                mapConfig.getCacheDeserializedValues().name(), mapConfig.getMergePolicyConfig().getPolicy(),
                mapConfig.getMergePolicyConfig().getBatchSize(), mapConfig.getInMemoryFormat().name(), listenerConfigs,
                partitionLostListenerConfigs, mapConfig.isStatisticsEnabled(), mapConfig.getSplitBrainProtectionName(),
                MapStoreConfigHolder.of(mapConfig.getMapStoreConfig(), serializationService),
                NearCacheConfigHolder.of(mapConfig.getNearCacheConfig(), serializationService),
                mapConfig.getWanReplicationRef(), mapConfig.getIndexConfigs(), mapConfig.getAttributeConfigs(),
                queryCacheConfigHolders, partitioningStrategyClassName, partitioningStrategy, mapConfig.getHotRestartConfig(),
                mapConfig.getEventJournalConfig(), mapConfig.getMerkleTreeConfig(), mapConfig.getMetadataPolicy().getId(),
                mapConfig.isPerEntryStatsEnabled(), mapConfig.getDataPersistenceConfig(), mapConfig.getTieredStoreConfig(),
                mapConfig.getPartitioningAttributeConfigs(), mapConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addCacheConfig(CacheSimpleConfig cacheConfig) {
        List<ListenerConfigHolder> partitionLostListenerConfigs =
                adaptListenerConfigs(cacheConfig.getPartitionLostListenerConfigs());

        DataPersistenceAndHotRestartMerger.merge(cacheConfig.getHotRestartConfig(), cacheConfig.getDataPersistenceConfig());

        ClientMessage request = DynamicConfigAddCacheConfigCodec.encodeRequest(cacheConfig.getName(), cacheConfig.getKeyType(),
                cacheConfig.getValueType(), cacheConfig.isStatisticsEnabled(), cacheConfig.isManagementEnabled(),
                cacheConfig.isReadThrough(), cacheConfig.isWriteThrough(), cacheConfig.getCacheLoaderFactory(),
                cacheConfig.getCacheWriterFactory(), cacheConfig.getCacheLoader(), cacheConfig.getCacheWriter(),
                cacheConfig.getBackupCount(), cacheConfig.getAsyncBackupCount(), cacheConfig.getInMemoryFormat().name(),
                cacheConfig.getSplitBrainProtectionName(), cacheConfig.getMergePolicyConfig().getPolicy(),
                cacheConfig.getMergePolicyConfig().getBatchSize(), cacheConfig.isDisablePerEntryInvalidationEvents(),
                partitionLostListenerConfigs,
                cacheConfig.getExpiryPolicyFactoryConfig() == null ? null
                        : cacheConfig.getExpiryPolicyFactoryConfig().getClassName(),
                cacheConfig.getExpiryPolicyFactoryConfig() == null ? null
                        : cacheConfig.getExpiryPolicyFactoryConfig().getTimedExpiryPolicyFactoryConfig(),
                cacheConfig.getCacheEntryListeners(),
                EvictionConfigHolder.of(cacheConfig.getEvictionConfig(), serializationService),
                cacheConfig.getWanReplicationRef(), cacheConfig.getEventJournalConfig(), cacheConfig.getHotRestartConfig(),
                cacheConfig.getMerkleTreeConfig(), cacheConfig.getDataPersistenceConfig(), cacheConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addQueueConfig(QueueConfig queueConfig) {
        List<ListenerConfigHolder> listenerConfigs = adaptListenerConfigs(queueConfig.getItemListenerConfigs());
        QueueStoreConfigHolder queueStoreConfigHolder =
                QueueStoreConfigHolder.of(queueConfig.getQueueStoreConfig(), serializationService);
        ClientMessage request = DynamicConfigAddQueueConfigCodec.encodeRequest(queueConfig.getName(), listenerConfigs,
                queueConfig.getBackupCount(), queueConfig.getAsyncBackupCount(), queueConfig.getMaxSize(),
                queueConfig.getEmptyQueueTtl(), queueConfig.isStatisticsEnabled(), queueConfig.getSplitBrainProtectionName(),
                queueStoreConfigHolder, queueConfig.getMergePolicyConfig().getPolicy(),
                queueConfig.getMergePolicyConfig().getBatchSize(), queueConfig.getPriorityComparatorClassName(),
                queueConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addListConfig(ListConfig listConfig) {
        List<ListenerConfigHolder> listenerConfigs = adaptListenerConfigs(listConfig.getItemListenerConfigs());
        ClientMessage request = DynamicConfigAddListConfigCodec.encodeRequest(listConfig.getName(), listenerConfigs,
                listConfig.getBackupCount(), listConfig.getAsyncBackupCount(), listConfig.getMaxSize(),
                listConfig.isStatisticsEnabled(), listConfig.getSplitBrainProtectionName(),
                listConfig.getMergePolicyConfig().getPolicy(), listConfig.getMergePolicyConfig().getBatchSize(),
                listConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addSetConfig(SetConfig setConfig) {
        List<ListenerConfigHolder> listenerConfigs = adaptListenerConfigs(setConfig.getItemListenerConfigs());
        ClientMessage request =
                DynamicConfigAddSetConfigCodec.encodeRequest(setConfig.getName(), listenerConfigs, setConfig.getBackupCount(),
                        setConfig.getAsyncBackupCount(), setConfig.getMaxSize(), setConfig.isStatisticsEnabled(),
                        setConfig.getSplitBrainProtectionName(), setConfig.getMergePolicyConfig().getPolicy(),
                        setConfig.getMergePolicyConfig().getBatchSize(), setConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        List<ListenerConfigHolder> listenerConfigHolders = adaptListenerConfigs(multiMapConfig.getEntryListenerConfigs());

        ClientMessage request = DynamicConfigAddMultiMapConfigCodec.encodeRequest(multiMapConfig.getName(),
                multiMapConfig.getValueCollectionType().toString(), listenerConfigHolders, multiMapConfig.isBinary(),
                multiMapConfig.getBackupCount(), multiMapConfig.getAsyncBackupCount(), multiMapConfig.isStatisticsEnabled(),
                multiMapConfig.getSplitBrainProtectionName(), multiMapConfig.getMergePolicyConfig().getPolicy(),
                multiMapConfig.getMergePolicyConfig().getBatchSize(), multiMapConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        List<ListenerConfigHolder> listenerConfigHolders = adaptListenerConfigs(replicatedMapConfig.getListenerConfigs());

        ClientMessage request = DynamicConfigAddReplicatedMapConfigCodec.encodeRequest(replicatedMapConfig.getName(),
                replicatedMapConfig.getInMemoryFormat().name(), replicatedMapConfig.isAsyncFillup(),
                replicatedMapConfig.isStatisticsEnabled(), replicatedMapConfig.getMergePolicyConfig().getPolicy(),
                listenerConfigHolders, replicatedMapConfig.getSplitBrainProtectionName(),
                replicatedMapConfig.getMergePolicyConfig().getBatchSize(), replicatedMapConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        RingbufferStoreConfigHolder ringbufferStoreConfig = null;
        if (ringbufferConfig.getRingbufferStoreConfig() != null && ringbufferConfig.getRingbufferStoreConfig().isEnabled()) {
            RingbufferStoreConfig storeConfig = ringbufferConfig.getRingbufferStoreConfig();
            ringbufferStoreConfig = RingbufferStoreConfigHolder.of(storeConfig, instance.getSerializationService());
        }
        ClientMessage request = DynamicConfigAddRingbufferConfigCodec.encodeRequest(ringbufferConfig.getName(),
                ringbufferConfig.getCapacity(), ringbufferConfig.getBackupCount(), ringbufferConfig.getAsyncBackupCount(),
                ringbufferConfig.getTimeToLiveSeconds(), ringbufferConfig.getInMemoryFormat().name(), ringbufferStoreConfig,
                ringbufferConfig.getSplitBrainProtectionName(), ringbufferConfig.getMergePolicyConfig().getPolicy(),
                ringbufferConfig.getMergePolicyConfig().getBatchSize(), ringbufferConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addTopicConfig(TopicConfig topicConfig) {
        List<ListenerConfigHolder> listenerConfigHolders = adaptListenerConfigs(topicConfig.getMessageListenerConfigs());

        ClientMessage request = DynamicConfigAddTopicConfigCodec.encodeRequest(topicConfig.getName(),
                topicConfig.isGlobalOrderingEnabled(), topicConfig.isStatisticsEnabled(), topicConfig.isMultiThreadingEnabled(),
                listenerConfigHolders, topicConfig.getUserCodeNamespace());
        invoke(request);
        return this;

    }

    @Override
    public Config addReliableTopicConfig(ReliableTopicConfig config) {
        List<ListenerConfigHolder> listenerConfigHolders = adaptListenerConfigs(config.getMessageListenerConfigs());
        Data executorData = serializationService.toData(config.getExecutor());
        ClientMessage request = DynamicConfigAddReliableTopicConfigCodec.encodeRequest(config.getName(), listenerConfigHolders,
                config.getReadBatchSize(), config.isStatisticsEnabled(), config.getTopicOverloadPolicy().name(), executorData,
                config.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        ClientMessage request = DynamicConfigAddExecutorConfigCodec.encodeRequest(executorConfig.getName(),
                executorConfig.getPoolSize(), executorConfig.getQueueCapacity(), executorConfig.isStatisticsEnabled(),
                executorConfig.getSplitBrainProtectionName(), executorConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addDurableExecutorConfig(DurableExecutorConfig durableExecutorConfig) {
        ClientMessage request = DynamicConfigAddDurableExecutorConfigCodec.encodeRequest(durableExecutorConfig.getName(),
                durableExecutorConfig.getPoolSize(), durableExecutorConfig.getDurability(), durableExecutorConfig.getCapacity(),
                durableExecutorConfig.getSplitBrainProtectionName(), durableExecutorConfig.isStatisticsEnabled(),
                durableExecutorConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        ClientMessage request = DynamicConfigAddScheduledExecutorConfigCodec.encodeRequest(scheduledExecutorConfig.getName(),
                scheduledExecutorConfig.getPoolSize(), scheduledExecutorConfig.getDurability(),
                scheduledExecutorConfig.getCapacity(), scheduledExecutorConfig.getSplitBrainProtectionName(),
                scheduledExecutorConfig.getMergePolicyConfig().getPolicy(),
                scheduledExecutorConfig.getMergePolicyConfig().getBatchSize(), scheduledExecutorConfig.isStatisticsEnabled(),
                scheduledExecutorConfig.getCapacityPolicy().getId(), scheduledExecutorConfig.getUserCodeNamespace());
        invoke(request);
        return this;
    }

    @Override
    public Config addCardinalityEstimatorConfig(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        ClientMessage request = DynamicConfigAddCardinalityEstimatorConfigCodec.encodeRequest(
                cardinalityEstimatorConfig.getName(), cardinalityEstimatorConfig.getBackupCount(),
                cardinalityEstimatorConfig.getAsyncBackupCount(), cardinalityEstimatorConfig.getSplitBrainProtectionName(),
                cardinalityEstimatorConfig.getMergePolicyConfig().getPolicy(),
                cardinalityEstimatorConfig.getMergePolicyConfig().getBatchSize());
        invoke(request);
        return this;
    }

    @Override
    public Config addPNCounterConfig(PNCounterConfig pnCounterConfig) {
        ClientMessage request =
                DynamicConfigAddPNCounterConfigCodec.encodeRequest(pnCounterConfig.getName(), pnCounterConfig.getReplicaCount(),
                        pnCounterConfig.isStatisticsEnabled(), pnCounterConfig.getSplitBrainProtectionName());
        invoke(request);
        return this;
    }

    @Override
    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        WanReplicationConfigTransformer transformer = new WanReplicationConfigTransformer(serializationService);
        WanConsumerConfigHolder consumerConfig = transformer.toHolder(wanReplicationConfig.getConsumerConfig());
        List<WanCustomPublisherConfigHolder> customPublisherConfigs = wanReplicationConfig.getCustomPublisherConfigs().stream()
                .filter(Objects::nonNull).map(transformer::toHolder).collect(Collectors.toList());
        List<WanBatchPublisherConfigHolder> batchPublisherConfigs = wanReplicationConfig.getBatchPublisherConfigs().stream()
                .filter(Objects::nonNull).map(transformer::toHolder).collect(Collectors.toList());
        ClientMessage request = DynamicConfigAddWanReplicationConfigCodec.encodeRequest(wanReplicationConfig.getName(),
                consumerConfig, customPublisherConfigs, batchPublisherConfigs);
        invoke(request);
        return this;
    }

    @Override
    public Config addSplitBrainProtectionConfig(SplitBrainProtectionConfig splitBrainProtectionConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config addListenerConfig(ListenerConfig listenerConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config addFlakeIdGeneratorConfig(FlakeIdGeneratorConfig flakeIdGeneratorConfig) {
        ClientMessage request = DynamicConfigAddFlakeIdGeneratorConfigCodec.encodeRequest(flakeIdGeneratorConfig.getName(),
                flakeIdGeneratorConfig.getPrefetchCount(), flakeIdGeneratorConfig.getPrefetchValidityMillis(),
                flakeIdGeneratorConfig.isStatisticsEnabled(), flakeIdGeneratorConfig.getNodeIdOffset(),
                flakeIdGeneratorConfig.getEpochStart(), flakeIdGeneratorConfig.getBitsSequence(),
                flakeIdGeneratorConfig.getBitsNodeId(), flakeIdGeneratorConfig.getAllowedFutureMillis());
        invoke(request);
        return this;
    }

    @Override
    public ClassLoader getClassLoader() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setClassLoader(ClassLoader classLoader) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ConfigPatternMatcher getConfigPatternMatcher() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public String getProperty(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setProperty(@Nonnull String name, @Nonnull String value) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public MemberAttributeConfig getMemberAttributeConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Properties getProperties() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setProperties(Properties properties) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public String getInstanceName() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setInstanceName(String instanceName) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public String getClusterName() {
        return instance.getClientConfig().getClusterName();
    }

    @Override
    public Config setClusterName(String clusterName) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public NetworkConfig getNetworkConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setNetworkConfig(NetworkConfig networkConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public AdvancedNetworkConfig getAdvancedNetworkConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setAdvancedNetworkConfig(AdvancedNetworkConfig advancedNetworkConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public MapConfig findMapConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public MapConfig getMapConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public CacheSimpleConfig findCacheConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public CacheSimpleConfig getCacheConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setCacheConfigs(Map<String, CacheSimpleConfig> cacheConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public QueueConfig getQueueConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ListConfig findListConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ListConfig getListConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, ListConfig> getListConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public SetConfig findSetConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public SetConfig getSetConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, SetConfig> getSetConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public MultiMapConfig getMultiMapConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setReplicatedMapConfigs(Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public RingbufferConfig getRingbufferConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public TopicConfig getTopicConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setReliableTopicConfigs(Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, TopicConfig> getTopicConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ExecutorConfig getExecutorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setExecutorConfigs(Map<String, ExecutorConfig> executorConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setDurableExecutorConfigs(Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setScheduledExecutorConfigs(Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setCardinalityEstimatorConfigs(Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public WanReplicationConfig getWanReplicationConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setWanReplicationConfigs(Map<String, WanReplicationConfig> wanReplicationConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, SplitBrainProtectionConfig> getSplitBrainProtectionConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public SplitBrainProtectionConfig getSplitBrainProtectionConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public SplitBrainProtectionConfig findSplitBrainProtectionConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setSplitBrainProtectionConfigs(Map<String, SplitBrainProtectionConfig> splitBrainProtectionConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ManagementCenterConfig getManagementCenterConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setManagementCenterConfig(ManagementCenterConfig managementCenterConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ServicesConfig getServicesConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public SecurityConfig getSecurityConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setSecurityConfig(SecurityConfig securityConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public List<ListenerConfig> getListenerConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public SerializationConfig getSerializationConfig() {
        return instance.getClientConfig().getSerializationConfig();
    }

    @Override
    public Config setSerializationConfig(SerializationConfig serializationConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public PartitionGroupConfig getPartitionGroupConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setPartitionGroupConfig(PartitionGroupConfig partitionGroupConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public HotRestartPersistenceConfig getHotRestartPersistenceConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setHotRestartPersistenceConfig(HotRestartPersistenceConfig hrConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public PersistenceConfig getPersistenceConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setPersistenceConfig(PersistenceConfig persistenceConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public DynamicConfigurationConfig getDynamicConfigurationConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setDynamicConfigurationConfig(DynamicConfigurationConfig dynamicConfigurationConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public DeviceConfig getDeviceConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public <T extends DeviceConfig> T getDeviceConfig(Class<T> clazz, String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, DeviceConfig> getDeviceConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setDeviceConfigs(Map<String, DeviceConfig> deviceConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config addDeviceConfig(DeviceConfig deviceConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public CRDTReplicationConfig getCRDTReplicationConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setCRDTReplicationConfig(CRDTReplicationConfig crdtReplicationConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ManagedContext getManagedContext() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setManagedContext(ManagedContext managedContext) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setUserContext(ConcurrentMap<String, Object> userContext) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public NativeMemoryConfig getNativeMemoryConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public URL getConfigurationUrl() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setConfigurationUrl(URL configurationUrl) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public File getConfigurationFile() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setConfigurationFile(File configurationFile) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public String getLicenseKey() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setLicenseKey(String licenseKey) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public boolean isLiteMember() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setLiteMember(boolean liteMember) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public UserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setUserCodeDeploymentConfig(UserCodeDeploymentConfig userCodeDeploymentConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public FlakeIdGeneratorConfig getFlakeIdGeneratorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setFlakeIdGeneratorConfigs(Map<String, FlakeIdGeneratorConfig> map) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public MapConfig getMapConfigOrNull(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public CacheSimpleConfig findCacheConfigOrNull(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public PNCounterConfig findPNCounterConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, PNCounterConfig> getPNCounterConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public PNCounterConfig getPNCounterConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setPNCounterConfigs(Map<String, PNCounterConfig> pnCounterConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public CPSubsystemConfig getCPSubsystemConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setCPSubsystemConfig(CPSubsystemConfig cpSubsystemConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    @Nonnull
    public Config setMetricsConfig(@Nonnull MetricsConfig metricsConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    @Nonnull
    public MetricsConfig getMetricsConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public Config setInstanceTrackingConfig(@Nonnull InstanceTrackingConfig instanceTrackingConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public InstanceTrackingConfig getInstanceTrackingConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    @Nonnull
    public SqlConfig getSqlConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    @Nonnull
    public Config setSqlConfig(@Nonnull SqlConfig sqlConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public JetConfig getJetConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public Config setJetConfig(JetConfig jetConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public AuditlogConfig getAuditlogConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public Config setAuditlogConfig(@Nonnull AuditlogConfig auditlogConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public IntegrityCheckerConfig getIntegrityCheckerConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public Config setIntegrityCheckerConfig(final IntegrityCheckerConfig config) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public LoadProtectionConfig getLoadProtectionConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public Config setLoadProtectionConfig(@Nonnull LoadProtectionConfig config) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, DataConnectionConfig> getDataConnectionConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config setDataConnectionConfigs(Map<String, DataConnectionConfig> dataConnectionConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Config addDataConnectionConfig(DataConnectionConfig dataConnectionConfig) {
        DataConnectionConfigValidator.validate(dataConnectionConfig);
        ClientMessage request = DynamicConfigAddDataConnectionConfigCodec.encodeRequest(dataConnectionConfig.getName(),
                dataConnectionConfig.getType(), dataConnectionConfig.isShared(), toMap(dataConnectionConfig.getProperties()));
        invoke(request);
        return this;
    }

    @Override
    public DataConnectionConfig getDataConnectionConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public DataConnectionConfig findDataConnectionConfig(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public TpcConfig getTpcConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public Config setTpcConfig(@Nonnull TpcConfig tpcConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    // S1185:S1612 A test (testDecorateAllPublicMethodsFromTest) forces this class to implement all of the parent methods, but
    // as we are overriding "namespaceConfig" with our own version, the parent implementation is sufficient
    @SuppressWarnings("squid:S1185")
    @Nonnull
    @Override
    public UserCodeNamespacesConfig getNamespacesConfig() {
        return super.getNamespacesConfig();
    }

    // S1185:S1612 A test (testDecorateAllPublicMethodsFromTest) forces this class to implement all of the parent methods, but
    // as we are overriding "namespaceConfig" with our own version, the parent implementation is sufficient
    @SuppressWarnings("squid:S1185")
    @Nonnull
    @Override
    public Config setNamespacesConfig(@Nonnull UserCodeNamespacesConfig userCodeNamespacesConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public RestConfig getRestConfig() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Nonnull
    @Override
    public Config setRestConfig(@Nonnull RestConfig restConfig) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public VectorCollectionConfig getVectorCollectionConfigOrNull(String name) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public Map<String, VectorCollectionConfig> getVectorCollectionConfigs() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    @Nonnull
    public Config addVectorCollectionConfig(@Nonnull VectorCollectionConfig vectorCollectionConfig) {
        ClientMessage request = DynamicConfigAddVectorCollectionConfigCodec.encodeRequest(
                vectorCollectionConfig.getName(),
                vectorCollectionConfig.getVectorIndexConfigs(),
                vectorCollectionConfig.getBackupCount(),
                vectorCollectionConfig.getAsyncBackupCount(),
                vectorCollectionConfig.getSplitBrainProtectionName(),
                vectorCollectionConfig.getMergePolicyConfig().getPolicy(),
                vectorCollectionConfig.getMergePolicyConfig().getBatchSize(),
                vectorCollectionConfig.getUserCodeNamespace()
        );
        invoke(request);
        return this;
    }

    @Override
    public Config setVectorCollectionConfigs(Map<String, VectorCollectionConfig> vectorConfigs) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERROR_MESSAGE);
    }

    @Override
    public String toString() {
        return "DynamicClusterConfig{instance=" + instance + "}";
    }

    protected void invoke(ClientMessage request) {
        try {
            ClientInvocation invocation = new ClientInvocation(instance, request, null);
            ClientInvocationFuture future = invocation.invoke();
            future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private List<ListenerConfigHolder> adaptListenerConfigs(List<? extends ListenerConfig> listenerConfigs) {
        List<ListenerConfigHolder> listenerConfigHolders = null;
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
            listenerConfigHolders = new ArrayList<>();
            for (ListenerConfig listenerConfig : listenerConfigs) {
                listenerConfigHolders.add(ListenerConfigHolder.of(listenerConfig, serializationService));
            }
        }
        return listenerConfigHolders;
    }
}
