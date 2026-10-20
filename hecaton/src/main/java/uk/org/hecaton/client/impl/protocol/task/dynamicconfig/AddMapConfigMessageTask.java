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

package uk.org.hecaton.client.impl.protocol.task.dynamicconfig;

import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec;
import uk.org.hecaton.config.CacheDeserializedValues;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EntryListenerConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapPartitionLostListenerConfig;
import uk.org.hecaton.config.MetadataPolicy;
import uk.org.hecaton.config.PartitioningStrategyConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

public class AddMapConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddMapConfigCodec.RequestParameters> {

    public AddMapConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected AddMapConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
            InternalSerializationService serializationService, ClientEngine clientEngine, Connection connection,
            NodeExtension nodeExtension, BuildInfo buildInfo, Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    @Override
    protected DynamicConfigAddMapConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddMapConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddMapConfigCodec.encodeResponse();
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:MethodLength"})
    protected IdentifiedDataSerializable getConfig() {
        MapConfig config = new MapConfig(parameters.name);
        config.setAsyncBackupCount(parameters.asyncBackupCount);
        config.setBackupCount(parameters.backupCount);
        config.setCacheDeserializedValues(CacheDeserializedValues.valueOf(parameters.cacheDeserializedValues));
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            config.setEntryListenerConfigs(
                    (List<EntryListenerConfig>) adaptListenerConfigs(parameters.listenerConfigs, parameters.userCodeNamespace));
        }
        if (parameters.merkleTreeConfig != null) {
            config.setMerkleTreeConfig(parameters.merkleTreeConfig);
        }
        if (parameters.eventJournalConfig != null) {
            config.setEventJournalConfig(parameters.eventJournalConfig);
        }
        if (parameters.hotRestartConfig != null) {
            config.setHotRestartConfig(parameters.hotRestartConfig);
        }

        config.setInMemoryFormat(InMemoryFormat.valueOf(parameters.inMemoryFormat));
        config.setAttributeConfigs(parameters.attributeConfigs);
        config.setReadBackupData(parameters.readBackupData);
        config.setStatisticsEnabled(parameters.statisticsEnabled);
        config.setPerEntryStatsEnabled(parameters.perEntryStatsEnabled);
        config.setIndexConfigs(parameters.indexConfigs);
        if (parameters.mapStoreConfig != null) {
            config.setMapStoreConfig(parameters.mapStoreConfig.asMapStoreConfig(serializationService,
                    parameters.userCodeNamespace));
        }
        config.setTimeToLiveSeconds(parameters.timeToLiveSeconds);
        config.setMaxIdleSeconds(parameters.maxIdleSeconds);
        if (parameters.evictionConfig != null) {
            config.setEvictionConfig(parameters.evictionConfig.asEvictionConfig(serializationService));
        }
        if (parameters.mergePolicy != null) {
            config.setMergePolicyConfig(mergePolicyConfig(parameters.mergePolicy, parameters.mergeBatchSize));
        }
        if (parameters.nearCacheConfig != null) {
            config.setNearCacheConfig(parameters.nearCacheConfig.asNearCacheConfig(serializationService));
        }
        config.setPartitioningStrategyConfig(getPartitioningStrategyConfig());
        if (parameters.partitionLostListenerConfigs != null && !parameters.partitionLostListenerConfigs.isEmpty()) {
            config.setPartitionLostListenerConfigs(
                    (List<MapPartitionLostListenerConfig>) adaptListenerConfigs(parameters.partitionLostListenerConfigs,
                            parameters.userCodeNamespace));
        }
        config.setSplitBrainProtectionName(parameters.splitBrainProtectionName);
        if (parameters.queryCacheConfigs != null && !parameters.queryCacheConfigs.isEmpty()) {
            List<QueryCacheConfig> queryCacheConfigs = new ArrayList<>(parameters.queryCacheConfigs.size());
            for (QueryCacheConfigHolder holder : parameters.queryCacheConfigs) {
                queryCacheConfigs.add(holder.asQueryCacheConfig(serializationService, parameters.userCodeNamespace));
            }
            config.setQueryCacheConfigs(queryCacheConfigs);
        }
        config.setWanReplicationRef(parameters.wanReplicationRef);
        config.setMetadataPolicy(MetadataPolicy.getById(parameters.metadataPolicy));
        if (parameters.isDataPersistenceConfigExists) {
            config.setDataPersistenceConfig(parameters.dataPersistenceConfig);
        }
        if (parameters.isTieredStoreConfigExists) {
            config.setTieredStoreConfig(parameters.tieredStoreConfig);
        }
        if (parameters.isPartitioningAttributeConfigsExists) {
            config.setPartitioningAttributeConfigs(parameters.partitioningAttributeConfigs);
        }
        if (parameters.isUserCodeNamespaceExists) {
            config.setUserCodeNamespace(parameters.userCodeNamespace);
        }
        return config;
    }

    private PartitioningStrategyConfig getPartitioningStrategyConfig() {
        if (parameters.partitioningStrategyClassName != null) {
            return new PartitioningStrategyConfig(parameters.partitioningStrategyClassName);
        } else if (parameters.partitioningStrategyImplementation != null) {
            PartitioningStrategy<?> partitioningStrategy =
                    serializationService.toObject(parameters.partitioningStrategyImplementation);
            return new PartitioningStrategyConfig(partitioningStrategy);
        } else {
            return null;
        }
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        return parameters.userCodeNamespace != null
                ? new UserCodeNamespacePermission(parameters.userCodeNamespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_MAP_CONFIG;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        MapConfig mapConfig = (MapConfig) config;
        return DynamicConfigurationAwareConfig.checkStaticConfigDoesNotExist(
                nodeConfig.getStaticConfig().getMapConfigs(),
                mapConfig.getName(), mapConfig);
    }
}
