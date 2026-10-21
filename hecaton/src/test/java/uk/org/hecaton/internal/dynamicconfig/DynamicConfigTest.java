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

package uk.org.hecaton.internal.dynamicconfig;

import uk.org.hecaton.cache.impl.event.CachePartitionLostEvent;
import uk.org.hecaton.cache.impl.event.CachePartitionLostListener;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.config.AttributeConfig;
import uk.org.hecaton.config.CacheDeserializedValues;
import uk.org.hecaton.config.CachePartitionLostListenerConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import uk.org.hecaton.config.CacheSimpleEntryListenerConfig;
import uk.org.hecaton.config.CardinalityEstimatorConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.config.NearCachePreloaderConfig;
import uk.org.hecaton.internal.diagnostics.Diagnostics;
import uk.org.hecaton.internal.diagnostics.DiagnosticsConfig;
import uk.org.hecaton.internal.diagnostics.DiagnosticsOutputType;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.config.EntryListenerConfig;
import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.config.FlakeIdGeneratorConfig;
import uk.org.hecaton.config.HotRestartConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.ItemListenerConfig;
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapPartitionLostListenerConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.config.MerkleTreeConfig;
import uk.org.hecaton.config.MetadataPolicy;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.NamedConfig;
import uk.org.hecaton.config.PNCounterConfig;
import uk.org.hecaton.config.PredicateConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.config.QueueStoreConfig;
import uk.org.hecaton.config.ReliableTopicConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.ScheduledExecutorConfig;
import uk.org.hecaton.config.SetConfig;
import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.map.MapPartitionLostEvent;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.map.listener.MapPartitionLostListener;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.ringbuffer.RingbufferStore;
import uk.org.hecaton.ringbuffer.RingbufferStoreFactory;
import uk.org.hecaton.spi.eviction.EvictableEntryView;
import uk.org.hecaton.spi.eviction.EvictionPolicyComparator;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.Message;
import uk.org.hecaton.topic.MessageListener;
import uk.org.hecaton.topic.TopicOverloadPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static uk.org.hecaton.config.MultiMapConfig.ValueCollectionType.LIST;
import static uk.org.hecaton.dataconnection.impl.DataConnectionTestUtil.DUMMY_TYPE;
import static uk.org.hecaton.test.TestConfigUtils.NON_DEFAULT_BACKUP_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigTest extends HecatonTestSupport {

    protected static final int INSTANCE_COUNT = 2;
    protected static final String NON_DEFAULT_MERGE_POLICY = "AnotherMergePolicy";
    protected static final int NON_DEFAULT_MERGE_BATCH_SIZE = 31415;

    private final String name = randomString();
    private HecatonInstance[] members;
    // add***Config is invoked on driver instance
    protected HecatonInstance driver;

    @Before
    public void setup() {
        members = newInstances();
        driver = getDriver();
    }

    protected HecatonInstance[] newInstances() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(INSTANCE_COUNT);
        return factory.newInstances();
    }

    protected HecatonInstance getDriver() {
        return members[members.length - 1];
    }

    @Test
    public void testAddWanReplicationConfig() {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName(name);

        getDriver().getConfig().addWanReplicationConfig(wanReplicationConfig);

        for (HecatonInstance member : members) {
            assertTrue(Accessors.getNode(member).getNodeEngine().getWanReplicationService().hasWanReplicationScheme(name));
        }
    }

    @Test
    public void testMultiMapConfig() {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name)
                .setBackupCount(4)
                .setAsyncBackupCount(2)
                .setStatisticsEnabled(true)
                .setBinary(true)
                .setValueCollectionType(LIST)
                .addEntryListenerConfig(new EntryListenerConfig("uk.org.hecaton.Listener", true, false));

        driver.getConfig().addMultiMapConfig(multiMapConfig);

        assertConfigurationsEqualOnAllMembers(multiMapConfig);
    }

    @Test
    public void testMultiMapConfig_whenEntryListenerConfigHasImplementation() {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name)
                .setBackupCount(4)
                .setAsyncBackupCount(2)
                .setStatisticsEnabled(true)
                .setBinary(true)
                .setValueCollectionType(LIST)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE))
                .addEntryListenerConfig(new EntryListenerConfig(new SampleEntryListener(), true, false));

        driver.getConfig().addMultiMapConfig(multiMapConfig);

        assertConfigurationsEqualOnAllMembers(multiMapConfig);
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(name, 4, 2)
                .setMergePolicyConfig(new MergePolicyConfig("uk.org.hecaton.spi.merge.DiscardMergePolicy", 20));
        driver.getConfig().addCardinalityEstimatorConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testListConfig() {
        ListConfig config = getListConfig();

        driver.getConfig().addListConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testListConfig_withItemListenerConfigs() {
        ListConfig config = getListConfig();
        config.addItemListenerConfig(getItemListenerConfig_byClassName());
        config.addItemListenerConfig(getItemListenerConfig_byImplementation());

        driver.getConfig().addListConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testExecutorConfig() {
        ExecutorConfig config = new ExecutorConfig(name, 7)
                .setStatisticsEnabled(true)
                .setQueueCapacity(13);

        driver.getConfig().addExecutorConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testDurableExecutorConfig() {
        DurableExecutorConfig config = new DurableExecutorConfig(name, 7, 3, 10, false, null);

        driver.getConfig().addDurableExecutorConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testScheduledExecutorConfig() {
        ScheduledExecutorConfig config = new ScheduledExecutorConfig(name, 2, 3, 10, null,
                new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE),
                ScheduledExecutorConfig.CapacityPolicy.PER_NODE, false, null);

        driver.getConfig().addScheduledExecutorConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig() {
        RingbufferConfig config = getRingbufferConfig();

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setClassName("uk.org.hecaton.Foo");

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryClassName("uk.org.hecaton.FactoryFoo");

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byStoreImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(new SampleRingbufferStore());

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryImplementation(new SampleRingbufferStoreFactory());

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testQueueConfig() {
        QueueConfig config = getQueueConfig();

        driver.getConfig().addQueueConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testQueueConfig_withListeners() {
        QueueConfig config = getQueueConfig_withListeners();

        driver.getConfig().addQueueConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapDefaultConfig() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withNonDefaultMergePolicy() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testSameReplicatedMapConfig_canBeAddedTwice() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name);
        driver.getConfig().addReplicatedMapConfig(config);
        ReplicatedMap map = driver.getReplicatedMap(name);
        driver.getConfig().addReplicatedMapConfig(config);
        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withListenerByClassName() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setStatisticsEnabled(true)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig(randomString(), true, false));
        config.getMergePolicyConfig().setPolicy("uk.org.hecaton.SomeMergePolicy");
        config.setAsyncFillup(true);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withListenerByImplementation() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setStatisticsEnabled(true)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig(new SampleEntryListener(), false, true));
        config.getMergePolicyConfig().setPolicy("uk.org.hecaton.SomeMergePolicy");
        config.setAsyncFillup(true);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testSetConfig() {
        SetConfig setConfig = getSetConfig(name);

        driver.getConfig().addSetConfig(setConfig);

        assertConfigurationsEqualOnAllMembers(setConfig);
    }

    @Test
    public void testMapConfig() {
        MapConfig config = getMapConfig();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testDefaultMapConfig() {
        MapConfig config = new MapConfig(name);

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withEntryListenerImplementation() {
        MapConfig config = getMapConfig_withEntryListenerImplementation();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withEntryListenerClassName() {
        MapConfig config = getMapConfig_withEntryListenerClassName();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig() {
        MapConfig config = getMapConfig_withQueryCacheConfig();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig_andEntryListenerConfigByClassName() {
        MapConfig config = getMapConfig_withQueryCacheConfig();
        config.getQueryCacheConfigs().get(0).addEntryListenerConfig(entryListenerConfigWithClassName());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig_andEntryListenerConfigByImplementation() {
        MapConfig config = getMapConfig_withQueryCacheConfig();
        config.getQueryCacheConfigs().get(0).addEntryListenerConfig(entryListenerConfigWithImplementation());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withMapPartitionLostListener_byClassName() {
        MapConfig config = getMapConfig();
        config.addMapPartitionLostListenerConfig(getMapPartitionLostListenerConfig_byClassName());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withMapPartitionLostListener_byImplementation() {
        MapConfig config = getMapConfig()
                .addMapPartitionLostListenerConfig(getMapPartitionLostListenerConfig_byImplementation());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withDifferentOrderOfIndexes() {
        MapConfig config = new MapConfig(name);
        IndexConfig idx1 = new IndexConfig(IndexType.SORTED, "foo");
        idx1.setName("idx1");
        IndexConfig idx2 = new IndexConfig(IndexType.SORTED, "bar");
        idx2.setName("idx2");
        config.setIndexConfigs(List.of(idx1, idx2));

        MapConfig reordered = new MapConfig(name);
        reordered.setIndexConfigs(List.of(idx2, idx1));

        driver.getConfig().addMapConfig(config);
        assertThatNoException().isThrownBy(() -> driver.getConfig().addMapConfig(reordered));

        assertConfigurationsEqualOnAllMembers(config);
        assertConfigurationsEqualOnAllMembers(reordered);
    }

    @Test
    public void testMapConfig_withNearCacheConfig() {
        MapConfig config = getMapConfig();
        NearCacheConfig nearCacheConfig = new NearCacheConfig(name)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setSerializeKeys(true)
                .setCacheLocalEntries(true)
                .setInvalidateOnChange(true)
                .setMaxIdleSeconds(100)
                .setTimeToLiveSeconds(200)
                .setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE)
                .setEvictionConfig(new EvictionConfig()
                        .setEvictionPolicy(EvictionPolicy.NONE)
                        .setSize(1000)
                        .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT))
                .setPreloaderConfig(new NearCachePreloaderConfig()
                        .setEnabled(false)
                        .setDirectory("/test")
                        .setStoreInitialDelaySeconds(1)
                        .setStoreIntervalSeconds(1));
        config.setNearCacheConfig(nearCacheConfig);

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testSetConfig_whenItemListenersConfigured() {
        SetConfig setConfig = getSetConfig(name);
        setConfig.addItemListenerConfig(getItemListenerConfig_byImplementation());
        setConfig.addItemListenerConfig(getItemListenerConfig_byClassName());

        driver.getConfig().addSetConfig(setConfig);

        assertConfigurationsEqualOnAllMembers(setConfig);
    }

    @Test
    public void testDefaultCacheConfig() {
        CacheSimpleConfig config = new CacheSimpleConfig()
                .setName(name);

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testSameCacheConfig_canBeAddedTwice() {
        CacheSimpleConfig config = new CacheSimpleConfig()
                .setName(name)
                .setBackupCount(NON_DEFAULT_BACKUP_COUNT);

        driver.getConfig().addCacheConfig(config);
        driver.getCacheManager().getCache(name);
        driver.getConfig().addCacheConfig(config);
    }

    @Test
    public void testCacheConfig() {
        CacheSimpleConfig config = getCacheConfig()
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig("expiryPolicyFactory"));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withEvictionPolicy_cacheLoaderAndWriter() {
        CacheSimpleConfig config = getCacheConfig()
                .setEvictionConfig(getEvictionConfigByPolicy())
                .setCacheLoader("uk.org.hecaton.CacheLoader")
                .setCacheWriter("uk.org.hecaton.CacheWriter")
                // also exercise alternative method to set expiry policy factory config
                .setExpiryPolicyFactory("expiryPolicyFactory");

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withEvictionPolicyImplementation_cacheLoaderAndWriterFactory() {
        CacheSimpleConfig config = getCacheConfig()
                .setEvictionConfig(getEvictionConfigByImplementation())
                .setCacheLoaderFactory("uk.org.hecaton.CacheLoaderFactory")
                .setCacheWriterFactory("uk.org.hecaton.CacheWriterFactory");

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withTimedExpiryPolicyFactory() {
        CacheSimpleConfig config = getCacheConfig()
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig(
                        new TimedExpiryPolicyFactoryConfig(TimedExpiryPolicyFactoryConfig.ExpiryPolicyType.TOUCHED,
                                new ExpiryPolicyFactoryConfig.DurationConfig(130, TimeUnit.SECONDS))
                ));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withPartitionLostListenerByClassName() {
        CacheSimpleConfig config = getCacheConfig()
                .addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig("partitionLostListener"));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withPartitionLostListenerByImplementation() {
        CacheSimpleConfig config = getCacheConfig()
                .addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig(
                        new SampleCachePartitionLostListener()
                ));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testFlakeIdGeneratorConfig() {
        FlakeIdGeneratorConfig config = new FlakeIdGeneratorConfig(randomName())
                .setPrefetchCount(123)
                .setPrefetchValidityMillis(456)
                .setEpochStart(789)
                .setNodeIdOffset(890)
                .setBitsNodeId(11)
                .setBitsSequence(22)
                .setStatisticsEnabled(false);
        driver.getConfig().addFlakeIdGeneratorConfig(config);
        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testTopicConfig() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(true)
                .setMultiThreadingEnabled(false)
                .setStatisticsEnabled(true);

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualOnAllMembers(topicConfig);
    }

    @Test
    public void testTopicConfig_whenListenerConfigByClassName() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(false)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getListenerConfig_byClassName());

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualOnAllMembers(topicConfig);
    }

    @Test
    public void testTopicConfig_whenListenerConfigByImplementation() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(false)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getMessageListenerConfig_byImplementation());

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualOnAllMembers(topicConfig);
    }

    @Test
    public void testReliableTopicConfig() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true);

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenListenerConfigByClassName() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getListenerConfig_byClassName());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenListenerConfigByImplementation() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getMessageListenerConfig_byImplementation());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenHasExecutor() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .setExecutor(new SampleExecutor());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testPNCounterConfig() {
        PNCounterConfig pnCounterConfig = new PNCounterConfig(name)
                .setReplicaCount(NON_DEFAULT_BACKUP_COUNT)
                .setStatisticsEnabled(false);

        driver.getConfig().addPNCounterConfig(pnCounterConfig);
        assertConfigurationsEqualOnAllMembers(pnCounterConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(PNCounterConfig config) {
        String name = config.getName();
        for (HecatonInstance instance : members) {
            PNCounterConfig registeredConfig = instance.getConfig().getPNCounterConfig(name);
            assertEquals(config, registeredConfig);
        }
    }

    @Test
    public void testDataConnectionConfig() {
        Properties properties = new Properties();
        properties.setProperty("prop1", "val1");
        properties.setProperty("prop2", "val2");
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig()
                .setName("some-name")
                .setType(DUMMY_TYPE)
                .setProperties(properties);


        driver.getConfig().addDataConnectionConfig(dataConnectionConfig);
        assertConfigurationsEqualOnAllMembers(dataConnectionConfig);
    }

    /**
     * Reproducer for <a href="https://github.com/hazelcast/hazelcast/issues/24533">GH issue</a>.
     */
    @Test
    public void testDataConnectionConfig_missingType() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();

        Exception ex = assertThrows(IllegalArgumentException.class,
                () -> driver.getConfig().addDataConnectionConfig(dataConnectionConfig));
        assertThat(ex.getMessage())
                .contains("Data connection type must be non-null and contain text")
                .contains("Data connection name must be non-null and contain text");
    }

    @Test
    public void testDiagnosticsConfig() {
        DiagnosticsConfig config = new DiagnosticsConfig()
                .setEnabled(true)
                .setMaxRolledFileSizeInMB(30)
                .setMaxRolledFileCount(5)
                .setIncludeEpochTime(false)
                .setLogDirectory(Path.of(Diagnostics.DIRECTORY.getDefaultValue(), "logs").toString())
                .setFileNamePrefix("fileNamePrefix")
                .setAutoOffDurationInMinutes(5)
                .setOutputType(DiagnosticsOutputType.STDOUT);

        ((DynamicConfigurationAwareConfig) driver.getConfig()).setDiagnosticsConfig(config);
        assertConfigurationsEqualOnAllMembers(config);
    }

    private void assertConfigurationsEqualOnAllMembers(DataConnectionConfig expectedConfig) {
        assertConfigurationsEqualOnAllMembers(expectedConfig, Config::getDataConnectionConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(CacheSimpleConfig config) {
        assertConfigurationsEqualOnAllMembers(config, Config::getCacheConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(QueueConfig queueConfig) {
        assertConfigurationsEqualOnAllMembers(queueConfig, Config::getQueueConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        assertConfigurationsEqualOnAllMembers(cardinalityEstimatorConfig, Config::getCardinalityEstimatorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(MultiMapConfig multiMapConfig) {
        assertConfigurationsEqualOnAllMembers(multiMapConfig, Config::getMultiMapConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ExecutorConfig executorConfig) {
        assertConfigurationsEqualOnAllMembers(executorConfig, Config::getExecutorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(RingbufferConfig ringbufferConfig) {
        assertConfigurationsEqualOnAllMembers(ringbufferConfig, Config::getRingbufferConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(DurableExecutorConfig durableExecutorConfig) {
        assertConfigurationsEqualOnAllMembers(durableExecutorConfig, Config::getDurableExecutorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ScheduledExecutorConfig scheduledExecutorConfig) {
        assertConfigurationsEqualOnAllMembers(scheduledExecutorConfig, Config::getScheduledExecutorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(SetConfig setConfig) {
        assertConfigurationsEqualOnAllMembers(setConfig, Config::getSetConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(MapConfig mapConfig) {
        assertConfigurationsEqualOnAllMembers(mapConfig, Config::getMapConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ReplicatedMapConfig replicatedMapConfig) {
        assertConfigurationsEqualOnAllMembers(replicatedMapConfig, Config::getReplicatedMapConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ListConfig listConfig) {
        assertConfigurationsEqualOnAllMembers(listConfig, Config::getListConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(FlakeIdGeneratorConfig config) {
        assertConfigurationsEqualOnAllMembers(config, Config::getFlakeIdGeneratorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(TopicConfig topicConfig) {
        assertConfigurationsEqualOnAllMembers(topicConfig, Config::getTopicConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ReliableTopicConfig reliableTopicConfig) {
        assertConfigurationsEqualOnAllMembers(reliableTopicConfig, Config::getReliableTopicConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(DiagnosticsConfig diagnosticsConfig) {
        for (HecatonInstance instance : members) {
            DiagnosticsConfig registeredConfig = ((DynamicConfigurationAwareConfig) instance.getConfig()).getDiagnosticsConfig();
            assertThat(registeredConfig).isEqualTo(diagnosticsConfig);
        }
    }

    private <T extends NamedConfig> void assertConfigurationsEqualOnAllMembers(T expectedConfig, BiFunction<Config, String, T> getterByName) {
        for (HecatonInstance instance : members) {
            T registeredConfig = getterByName.apply(instance.getConfig(), expectedConfig.getName());
            assertThat(registeredConfig).isEqualTo(expectedConfig);
        }
    }

    private CacheSimpleConfig getCacheConfig() {
        CacheSimpleEntryListenerConfig entryListenerConfig = new CacheSimpleEntryListenerConfig();
        entryListenerConfig.setCacheEntryListenerFactory("CacheEntryListenerFactory");
        entryListenerConfig.setSynchronous(true);
        entryListenerConfig.setOldValueRequired(true);
        entryListenerConfig.setCacheEntryEventFilterFactory("CacheEntryEventFilterFactory");

        CacheSimpleConfig config = new CacheSimpleConfig()
                .setName(name)
                .setSplitBrainProtectionName("split-brain-protection")
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setBackupCount(3)
                .setAsyncBackupCount(2)
                .addEntryListenerConfig(entryListenerConfig)
                .setStatisticsEnabled(true)
                .setManagementEnabled(true)
                .setKeyType("keyType")
                .setValueType("valueType")
                .setReadThrough(true)
                .setWriteThrough(true)
                .setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true))
                .setEventJournalConfig(new EventJournalConfig().setEnabled(true)
                        .setCapacity(42)
                        .setTimeToLiveSeconds(52));

        config.setWanReplicationRef(new WanReplicationRef(randomName(), "uk.org.hecaton.MergePolicy",
                Collections.singletonList("filter"), true));
        config.getMergePolicyConfig().setPolicy("mergePolicy");
        config.setDisablePerEntryInvalidationEvents(true);
        return config;
    }

    private EvictionConfig getEvictionConfigByPolicy() {
        return new EvictionConfig().setSize(39)
                .setMaxSizePolicy(ENTRY_COUNT)
                .setEvictionPolicy(EvictionPolicy.RANDOM);
    }

    private EvictionConfig getEvictionConfigByImplementation() {
        return new EvictionConfig().setSize(39)
                .setMaxSizePolicy(ENTRY_COUNT)
                .setComparator(new SampleEvictionPolicyComparator());
    }

    private SetConfig getSetConfig(String name) {
        return new SetConfig(name)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setMaxSize(99)
                .setStatisticsEnabled(true)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY,
                        NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    private MapConfig getMapConfig() {
        MapConfig mapConfig = new MapConfig(name);
        mapConfig.setAsyncBackupCount(3)
                .setBackupCount(2)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setMerkleTreeConfig(new MerkleTreeConfig().setEnabled(true).setDepth(15))
                .setEventJournalConfig(new EventJournalConfig().setEnabled(true)
                        .setCapacity(42)
                        .setTimeToLiveSeconds(52))
                .setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE))
                .setTimeToLiveSeconds(220)
                .setMaxIdleSeconds(110)
                .setSplitBrainProtectionName(randomString())
                .addAttributeConfig(new AttributeConfig("attributeName", "com.attribute.extractor"))
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "attr"))
                .setMetadataPolicy(MetadataPolicy.OFF)
                .setReadBackupData(true)
                .setStatisticsEnabled(false)
                .setPerEntryStatsEnabled(true);

        mapConfig.getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.RANDOM)
                .setSize(4096)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE);
        return mapConfig;
    }

    private MapConfig getMapConfig_withEntryListenerImplementation() {
        return new MapConfig(name).addEntryListenerConfig(entryListenerConfigWithImplementation());
    }

    private MapConfig getMapConfig_withEntryListenerClassName() {
        return new MapConfig(name).addEntryListenerConfig(entryListenerConfigWithClassName());
    }

    private MapConfig getMapConfig_withQueryCacheConfig() {
        return new MapConfig(name).addQueryCacheConfig(getQueryCacheConfig());
    }

    private EntryListenerConfig entryListenerConfigWithImplementation() {
        return new EntryListenerConfig(new SampleEntryListener(), false, true);
    }

    private EntryListenerConfig entryListenerConfigWithClassName() {
        return new EntryListenerConfig("uk.org.hecaton.someListener", false, true);
    }

    private QueryCacheConfig getQueryCacheConfig() {
        return new QueryCacheConfig(randomName())
                .setBatchSize(131)
                .setDelaySeconds(98)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setBufferSize(873)
                .setPopulate(true)
                .setIncludeValue(true)
                .setCoalesce(true)
                .setSerializeKeys(true)
                .setPredicateConfig(new PredicateConfig("uk.org.hecaton.Predicate"))
                .setEvictionConfig(new EvictionConfig().setSize(32)
                        .setMaxSizePolicy(ENTRY_COUNT)
                        .setComparatorClassName("uk.org.hecaton.Comparator"));
    }

    private MapPartitionLostListenerConfig getMapPartitionLostListenerConfig_byClassName() {
        return new MapPartitionLostListenerConfig("uk.org.hecaton.PartitionLostListener");
    }

    private MapPartitionLostListenerConfig getMapPartitionLostListenerConfig_byImplementation() {
        return new MapPartitionLostListenerConfig(new SamplePartitionLostListener());
    }

    private ListConfig getListConfig() {
        return new ListConfig(name)
                .setStatisticsEnabled(true)
                .setMaxSize(99)
                .setBackupCount(4)
                .setAsyncBackupCount(2)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    private RingbufferConfig getRingbufferConfig() {
        return new RingbufferConfig(name)
                .setTimeToLiveSeconds(59)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCapacity(33)
                .setBackupCount(4)
                .setAsyncBackupCount(2)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    public QueueConfig getQueueConfig() {
        String name = randomName();
        return new QueueConfig(name)
                .setBackupCount(2)
                .setAsyncBackupCount(2)
                // no explicit max size - let's test encoding of the default value
                .setEmptyQueueTtl(10)
                .setQueueStoreConfig(new QueueStoreConfig().setClassName("foo.bar.ImplName").setEnabled(true))
                .setStatisticsEnabled(false)
                .setSplitBrainProtectionName("mySplitBrainProtection")
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    public QueueConfig getQueueConfig_withListeners() {
        String name = randomName();
        return new QueueConfig(name)
                .addItemListenerConfig(getItemListenerConfig_byClassName())
                .addItemListenerConfig(getItemListenerConfig_byImplementation())
                .setBackupCount(2)
                .setAsyncBackupCount(2)
                .setMaxSize(1000)
                .setEmptyQueueTtl(10)
                .setQueueStoreConfig(new QueueStoreConfig().setClassName("foo.bar.ImplName").setEnabled(true))
                .setStatisticsEnabled(false)
                .setSplitBrainProtectionName("mySplitBrainProtection");
    }

    private ListenerConfig getListenerConfig_byClassName() {
        return new ListenerConfig("uk.org.hecaton.ListenerClassName");
    }

    private ListenerConfig getMessageListenerConfig_byImplementation() {
        return new ListenerConfig(new SampleMessageListener());
    }

    private ItemListenerConfig getItemListenerConfig_byClassName() {
        return new ItemListenerConfig("uk.org.hecaton.ItemListener", true);
    }

    private ItemListenerConfig getItemListenerConfig_byImplementation() {
        return new ItemListenerConfig(new SampleItemListener(), true);
    }

    public static class SampleEntryListener implements EntryAddedListener, Serializable {

        @Override
        public void entryAdded(EntryEvent event) {
        }

        @Override
        public int hashCode() {
            return 31;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SampleEntryListener;
        }
    }

    public static class SampleItemListener implements ItemListener, Serializable {

        @Override
        public void itemAdded(ItemEvent item) {
        }

        @Override
        public void itemRemoved(ItemEvent item) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleItemListener);
        }

        @Override
        public int hashCode() {
            return 33;
        }
    }

    public static class SampleRingbufferStore implements RingbufferStore, Serializable {

        @Override
        public void store(long sequence, Object data) {
        }

        @Override
        public void storeAll(long firstItemSequence, Object[] items) {
        }

        @Override
        public Object load(long sequence) {
            return null;
        }

        @Override
        public long getLargestSequence() {
            return 0;
        }

        @Override
        public int hashCode() {
            return 33;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleRingbufferStore);
        }
    }

    public static class SampleRingbufferStoreFactory implements RingbufferStoreFactory, Serializable {

        @Override
        public RingbufferStore newRingbufferStore(String name, Properties properties) {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleRingbufferStoreFactory);
        }
    }

    public static class SamplePartitionLostListener implements MapPartitionLostListener, Serializable {

        @Override
        public void partitionLost(MapPartitionLostEvent event) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SamplePartitionLostListener);
        }
    }

    public static class SampleEvictionPolicyComparator
            implements EvictionPolicyComparator<Object, Object, EvictableEntryView<Object, Object>> {

        @Override
        public int compare(EvictableEntryView e1, EvictableEntryView e2) {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleEvictionPolicyComparator);
        }
    }

    public static class SampleCachePartitionLostListener implements CachePartitionLostListener, Serializable {

        @Override
        public void partitionLost(CachePartitionLostEvent event) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleCachePartitionLostListener);
        }
    }

    private static class SampleMessageListener implements MessageListener<Object>, Serializable {

        @Override
        public void onMessage(Message<Object> message) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleMessageListener);
        }
    }

    private static class SampleExecutor implements Executor, Serializable {

        @Override
        public void execute(Runnable command) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleExecutor);
        }
    }
}
