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

import uk.org.hecaton.config.AttributeConfig;
import uk.org.hecaton.config.AwsConfig;
import uk.org.hecaton.config.AzureConfig;
import uk.org.hecaton.config.BTreeIndexConfig;
import uk.org.hecaton.config.BitmapIndexOptions;
import uk.org.hecaton.config.CachePartitionLostListenerConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import uk.org.hecaton.config.CacheSimpleEntryListenerConfig;
import uk.org.hecaton.config.CardinalityEstimatorConfig;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.config.DataPersistenceConfig;
import uk.org.hecaton.internal.diagnostics.DiagnosticsConfig;
import uk.org.hecaton.config.DiscoveryConfig;
import uk.org.hecaton.config.DiscoveryStrategyConfig;
import uk.org.hecaton.config.DiskTierConfig;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.config.EntryListenerConfig;
import uk.org.hecaton.config.EurekaConfig;
import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.config.FlakeIdGeneratorConfig;
import uk.org.hecaton.config.GcpConfig;
import uk.org.hecaton.config.HotRestartConfig;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.ItemListenerConfig;
import uk.org.hecaton.config.KubernetesConfig;
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapPartitionLostListenerConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.config.MemoryTierConfig;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.config.MerkleTreeConfig;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.config.NearCachePreloaderConfig;
import uk.org.hecaton.config.PNCounterConfig;
import uk.org.hecaton.config.PartitioningAttributeConfig;
import uk.org.hecaton.config.PartitioningStrategyConfig;
import uk.org.hecaton.config.PermissionConfig;
import uk.org.hecaton.config.PredicateConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.config.QueueStoreConfig;
import uk.org.hecaton.config.ReliableTopicConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.RingbufferStoreConfig;
import uk.org.hecaton.config.ScheduledExecutorConfig;
import uk.org.hecaton.config.SetConfig;
import uk.org.hecaton.config.SplitBrainProtectionConfig;
import uk.org.hecaton.config.SplitBrainProtectionListenerConfig;
import uk.org.hecaton.config.TieredStoreConfig;
import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.config.UserCodeNamespaceConfig;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.config.WanCustomPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.config.WanSyncConfig;
import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.config.vector.VectorIndexConfig;
import uk.org.hecaton.internal.dynamicconfig.AddDynamicConfigOperation;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigPreJoinOperation;
import uk.org.hecaton.internal.namespace.impl.ResourceDefinitionImpl;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.ArrayDataSerializableFactory;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY_ID;

/**
 * DataSerializerHook for uk.org.hecaton.config classes
 */
@SuppressWarnings({"JavadocVariable", "deprecation", "ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
public final class ConfigDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(CONFIG_DS_FACTORY, CONFIG_DS_FACTORY_ID);

    public static final int WAN_REPLICATION_CONFIG = 0;
    public static final int WAN_CUSTOM_PUBLISHER_CONFIG = 1;
    public static final int WAN_BATCH_PUBLISHER_CONFIG = 2;
    public static final int WAN_CONSUMER_CONFIG = 3;
    public static final int NEAR_CACHE_CONFIG = 4;
    public static final int NEAR_CACHE_PRELOADER_CONFIG = 5;
    public static final int ADD_DYNAMIC_CONFIG_OP = 6;
    public static final int DYNAMIC_CONFIG_PRE_JOIN_OP = 7;
    public static final int MULTIMAP_CONFIG = 8;
    public static final int LISTENER_CONFIG = 9;
    public static final int ENTRY_LISTENER_CONFIG = 10;
    public static final int MAP_CONFIG = 11;
    public static final int RANDOM_EVICTION_POLICY = 12;
    public static final int LFU_EVICTION_POLICY = 13;
    public static final int LRU_EVICTION_POLICY = 14;
    public static final int MAP_STORE_CONFIG = 15;
    public static final int MAP_PARTITION_LOST_LISTENER_CONFIG = 16;
    public static final int INDEX_CONFIG = 17;
    public static final int MAP_ATTRIBUTE_CONFIG = 18;
    public static final int QUERY_CACHE_CONFIG = 19;
    public static final int PREDICATE_CONFIG = 20;
    public static final int PARTITION_STRATEGY_CONFIG = 21;
    public static final int HOT_RESTART_CONFIG = 22;
    public static final int TOPIC_CONFIG = 23;
    public static final int RELIABLE_TOPIC_CONFIG = 24;
    public static final int ITEM_LISTENER_CONFIG = 25;
    public static final int QUEUE_STORE_CONFIG = 26;
    public static final int QUEUE_CONFIG = 27;
    public static final int LIST_CONFIG = 28;
    public static final int SET_CONFIG = 29;
    public static final int EXECUTOR_CONFIG = 30;
    public static final int DURABLE_EXECUTOR_CONFIG = 31;
    public static final int SCHEDULED_EXECUTOR_CONFIG = 32;
    public static final int REPLICATED_MAP_CONFIG = 33;
    public static final int RINGBUFFER_CONFIG = 34;
    public static final int RINGBUFFER_STORE_CONFIG = 35;
    public static final int CARDINALITY_ESTIMATOR_CONFIG = 36;
    public static final int SIMPLE_CACHE_CONFIG = 37;
    public static final int SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG = 38;
    public static final int SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG = 39;
    public static final int SIMPLE_CACHE_CONFIG_DURATION_CONFIG = 40;
    public static final int SPLIT_BRAIN_PROTECTION_CONFIG = 41;
    public static final int MAP_LISTENER_TO_ENTRY_LISTENER_ADAPTER = 42;
    public static final int EVENT_JOURNAL_CONFIG = 43;
    public static final int SPLIT_BRAIN_PROTECTION_LISTENER_CONFIG = 44;
    public static final int CACHE_PARTITION_LOST_LISTENER_CONFIG = 45;
    public static final int SIMPLE_CACHE_ENTRY_LISTENER_CONFIG = 46;
    public static final int FLAKE_ID_GENERATOR_CONFIG = 47;
    public static final int MERGE_POLICY_CONFIG = 48;
    public static final int PN_COUNTER_CONFIG = 49;
    public static final int MERKLE_TREE_CONFIG = 50;
    public static final int WAN_SYNC_CONFIG = 51;
    public static final int KUBERNETES_CONFIG = 52;
    public static final int EUREKA_CONFIG = 53;
    public static final int GCP_CONFIG = 54;
    public static final int AZURE_CONFIG = 55;
    public static final int AWS_CONFIG = 56;
    public static final int DISCOVERY_CONFIG = 57;
    public static final int DISCOVERY_STRATEGY_CONFIG = 58;
    public static final int WAN_REPLICATION_REF = 59;
    public static final int EVICTION_CONFIG = 60;
    public static final int PERMISSION_CONFIG = 61;
    public static final int BITMAP_INDEX_OPTIONS = 62;
    public static final int DATA_PERSISTENCE_CONFIG = 63;
    public static final int TIERED_STORE_CONFIG = 64;
    public static final int MEMORY_TIER_CONFIG = 65;
    public static final int DISK_TIER_CONFIG = 66;
    public static final int BTREE_INDEX_CONFIG = 67;
    public static final int DATA_CONNECTION_CONFIG = 68;
    public static final int PARTITION_ATTRIBUTE_CONFIG = 69;
    public static final int NAMESPACE_CONFIG = 70;
    public static final int RESOURCE_DEFINITION = 71;
    public static final short VECTOR_COLLECTION_CONFIG = 72;
    public static final short VECTOR_INDEX_CONFIG = 73;
    public static final int DIAGNOSTICS_CONFIG = 74;

    private static final int LEN = DIAGNOSTICS_CONFIG + 1;



    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    @SuppressWarnings("MethodLength")
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[WAN_REPLICATION_CONFIG] = WanReplicationConfig::new;
        constructors[WAN_CONSUMER_CONFIG] = WanConsumerConfig::new;
        constructors[WAN_CUSTOM_PUBLISHER_CONFIG] = WanCustomPublisherConfig::new;
        constructors[WAN_BATCH_PUBLISHER_CONFIG] = WanBatchPublisherConfig::new;
        constructors[NEAR_CACHE_CONFIG] = NearCacheConfig::new;
        constructors[NEAR_CACHE_PRELOADER_CONFIG] = NearCachePreloaderConfig::new;
        constructors[ADD_DYNAMIC_CONFIG_OP] = AddDynamicConfigOperation::new;
        constructors[DYNAMIC_CONFIG_PRE_JOIN_OP] = DynamicConfigPreJoinOperation::new;
        constructors[MULTIMAP_CONFIG] = MultiMapConfig::new;
        constructors[LISTENER_CONFIG] = ListenerConfig::new;
        constructors[ENTRY_LISTENER_CONFIG] = EntryListenerConfig::new;
        constructors[MAP_CONFIG] = MapConfig::new;
        constructors[MAP_STORE_CONFIG] = MapStoreConfig::new;
        constructors[MAP_PARTITION_LOST_LISTENER_CONFIG] = MapPartitionLostListenerConfig::new;
        constructors[INDEX_CONFIG] = IndexConfig::new;
        constructors[MAP_ATTRIBUTE_CONFIG] = AttributeConfig::new;
        constructors[QUERY_CACHE_CONFIG] = QueryCacheConfig::new;
        constructors[PREDICATE_CONFIG] = PredicateConfig::new;
        constructors[PARTITION_STRATEGY_CONFIG] = PartitioningStrategyConfig::new;
        constructors[HOT_RESTART_CONFIG] = HotRestartConfig::new;
        constructors[TOPIC_CONFIG] = TopicConfig::new;
        constructors[RELIABLE_TOPIC_CONFIG] = ReliableTopicConfig::new;
        constructors[ITEM_LISTENER_CONFIG] = ItemListenerConfig::new;
        constructors[QUEUE_STORE_CONFIG] = QueueStoreConfig::new;
        constructors[QUEUE_CONFIG] = QueueConfig::new;
        constructors[LIST_CONFIG] = ListConfig::new;
        constructors[SET_CONFIG] = SetConfig::new;
        constructors[EXECUTOR_CONFIG] = ExecutorConfig::new;
        constructors[DURABLE_EXECUTOR_CONFIG] = DurableExecutorConfig::new;
        constructors[SCHEDULED_EXECUTOR_CONFIG] = ScheduledExecutorConfig::new;
        constructors[REPLICATED_MAP_CONFIG] = ReplicatedMapConfig::new;
        constructors[RINGBUFFER_CONFIG] = RingbufferConfig::new;
        constructors[RINGBUFFER_STORE_CONFIG] = RingbufferStoreConfig::new;
        constructors[CARDINALITY_ESTIMATOR_CONFIG] = CardinalityEstimatorConfig::new;
        constructors[SIMPLE_CACHE_CONFIG] = CacheSimpleConfig::new;
        constructors[SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG] =
                ExpiryPolicyFactoryConfig::new;
        constructors[SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG] =
                TimedExpiryPolicyFactoryConfig::new;
        constructors[SIMPLE_CACHE_CONFIG_DURATION_CONFIG] =
                DurationConfig::new;
        constructors[SPLIT_BRAIN_PROTECTION_CONFIG] = SplitBrainProtectionConfig::new;
        constructors[EVENT_JOURNAL_CONFIG] = EventJournalConfig::new;
        constructors[SPLIT_BRAIN_PROTECTION_LISTENER_CONFIG] = SplitBrainProtectionListenerConfig::new;
        constructors[CACHE_PARTITION_LOST_LISTENER_CONFIG] = CachePartitionLostListenerConfig::new;
        constructors[SIMPLE_CACHE_ENTRY_LISTENER_CONFIG] = CacheSimpleEntryListenerConfig::new;
        constructors[FLAKE_ID_GENERATOR_CONFIG] = FlakeIdGeneratorConfig::new;
        constructors[MERGE_POLICY_CONFIG] = MergePolicyConfig::new;
        constructors[PN_COUNTER_CONFIG] = PNCounterConfig::new;
        constructors[MERKLE_TREE_CONFIG] = MerkleTreeConfig::new;
        constructors[WAN_SYNC_CONFIG] = WanSyncConfig::new;
        constructors[KUBERNETES_CONFIG] = KubernetesConfig::new;
        constructors[EUREKA_CONFIG] = EurekaConfig::new;
        constructors[GCP_CONFIG] = GcpConfig::new;
        constructors[AZURE_CONFIG] = AzureConfig::new;
        constructors[AWS_CONFIG] = AwsConfig::new;
        constructors[DISCOVERY_CONFIG] = DiscoveryConfig::new;
        constructors[DISCOVERY_STRATEGY_CONFIG] = DiscoveryStrategyConfig::new;
        constructors[WAN_REPLICATION_REF] = WanReplicationRef::new;
        constructors[EVICTION_CONFIG] = EvictionConfig::new;
        constructors[PERMISSION_CONFIG] = PermissionConfig::new;
        constructors[BITMAP_INDEX_OPTIONS] = BitmapIndexOptions::new;
        constructors[DATA_PERSISTENCE_CONFIG] = DataPersistenceConfig::new;
        constructors[TIERED_STORE_CONFIG] = TieredStoreConfig::new;
        constructors[MEMORY_TIER_CONFIG] = MemoryTierConfig::new;
        constructors[DISK_TIER_CONFIG] = DiskTierConfig::new;
        constructors[BTREE_INDEX_CONFIG] = BTreeIndexConfig::new;
        constructors[DATA_CONNECTION_CONFIG] = DataConnectionConfig::new;
        constructors[PARTITION_ATTRIBUTE_CONFIG] = PartitioningAttributeConfig::new;
        constructors[NAMESPACE_CONFIG] = UserCodeNamespaceConfig::new;
        constructors[RESOURCE_DEFINITION] = ResourceDefinitionImpl::new;
        constructors[VECTOR_COLLECTION_CONFIG] = VectorCollectionConfig::new;
        constructors[VECTOR_INDEX_CONFIG] = VectorIndexConfig::new;
        constructors[DIAGNOSTICS_CONFIG] = DiagnosticsConfig::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
