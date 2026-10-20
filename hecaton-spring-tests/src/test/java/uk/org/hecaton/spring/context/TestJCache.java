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

package uk.org.hecaton.spring.context;

import uk.org.hecaton.config.CachePartitionLostListenerConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.MergePolicyConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spring.CustomSpringExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests for JCache parser.
 */
@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"test-jcache-application-context.xml"})
class TestJCache {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private HecatonInstance instance1;

    @BeforeAll
    @AfterAll
    static void tearDown() {
        Hecaton.shutdownAll();
    }

    @Test
    void testContextInitializedSuccessfully() {
        assertNotNull(context);
    }

    @Test
    void testConfig() {
        assertNotNull(instance1);

        CacheSimpleConfig simpleConfig = instance1.getConfig().getCacheConfigs().get("cache1");

        assertNotNull(simpleConfig);

        assertEquals(1, simpleConfig.getAsyncBackupCount());
        assertEquals(2, simpleConfig.getBackupCount());
        assertEquals("java.lang.Integer", simpleConfig.getKeyType());
        assertEquals("java.lang.String", simpleConfig.getValueType());
        assertTrue(simpleConfig.isStatisticsEnabled());
        assertTrue(simpleConfig.isManagementEnabled());
        assertTrue(simpleConfig.isReadThrough());
        assertTrue(simpleConfig.isWriteThrough());
        assertEquals("uk.org.hecaton.cache.MyCacheLoaderFactory", simpleConfig.getCacheLoaderFactory());
        assertEquals("uk.org.hecaton.cache.MyCacheWriterFactory", simpleConfig.getCacheWriterFactory());
        assertEquals("uk.org.hecaton.cache.MyExpiryPolicyFactory",
                simpleConfig.getExpiryPolicyFactoryConfig().getClassName());
        assertEquals(InMemoryFormat.OBJECT, simpleConfig.getInMemoryFormat());
        assertNotNull(simpleConfig.getEvictionConfig());
        assertEquals(50, simpleConfig.getEvictionConfig().getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT,
                simpleConfig.getEvictionConfig().getMaxSizePolicy());
        assertEquals(EvictionPolicy.LRU, simpleConfig.getEvictionConfig().getEvictionPolicy());
    }

    @Test
    void cacheConfigXmlTest_TimedCreatedExpiryPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedCreatedExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedCreatedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedCreatedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.CREATED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(1, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.DAYS, durationConfig.getTimeUnit());
    }

    @Test
    void cacheConfigXmlTest_TimedAccessedExpiryPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedAccessedExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedAccessedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedAccessedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.ACCESSED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(2, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.HOURS, durationConfig.getTimeUnit());
    }

    @Test
    void cacheConfigXmlTest_TimedModifiedExpiryPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedModifiedExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedModifiedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedModifiedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.MODIFIED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(3, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.MINUTES, durationConfig.getTimeUnit());
    }

    @Test
    void cacheConfigXmlTest_TimedModifiedTouchedPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedTouchedExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedTouchedExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedTouchedExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNotNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.TOUCHED, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
        assertEquals(4, durationConfig.getDurationAmount());
        assertEquals(TimeUnit.SECONDS, durationConfig.getTimeUnit());
    }

    @Test
    void cacheConfigXmlTest_TimedEternalTouchedPolicyFactory() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithTimedEternalExpiryPolicyFactoryConfig =
                config.getCacheConfig("cacheWithTimedEternalExpiryPolicyFactory");
        ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                cacheWithTimedEternalExpiryPolicyFactoryConfig.getExpiryPolicyFactoryConfig();
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig =
                expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
        DurationConfig durationConfig = timedExpiryPolicyFactoryConfig.getDurationConfig();

        assertNotNull(expiryPolicyFactoryConfig);
        assertNotNull(timedExpiryPolicyFactoryConfig);
        assertNull(durationConfig);
        assertNull(expiryPolicyFactoryConfig.getClassName());

        assertEquals(ExpiryPolicyType.ETERNAL, timedExpiryPolicyFactoryConfig.getExpiryPolicyType());
    }

    @Test
    void cacheConfigXmlTest_PartitionLostListener() {
        Config config = instance1.getConfig();

        CacheSimpleConfig cacheWithPartitionLostListenerConfig =
                config.getCacheConfig("cacheWithPartitionLostListener");
        List<CachePartitionLostListenerConfig> partitionLostListenerConfigs =
                cacheWithPartitionLostListenerConfig.getPartitionLostListenerConfigs();

        assertNotNull(partitionLostListenerConfigs);
        assertEquals(1, partitionLostListenerConfigs.size());
        assertEquals("DummyCachePartitionLostListenerImpl", partitionLostListenerConfigs.get(0).getClassName());
        assertNotNull(partitionLostListenerConfigs);
        assertEquals(1, partitionLostListenerConfigs.size());
        assertEquals("DummyCachePartitionLostListenerImpl", partitionLostListenerConfigs.get(0).getClassName());
    }

    @Test
    void cacheConfigXmlTest_ClusterSplitBrainProtection() {
        assertNotNull(instance1);

        CacheSimpleConfig simpleConfig = instance1.getConfig().getCacheConfig("cacheWithSplitBrainProtectionRef");

        assertNotNull(simpleConfig);

        assertEquals("cacheSplitBrainProtectionRefString", simpleConfig.getSplitBrainProtectionName());
    }

    @Test
    void cacheConfigXmlTest_DefaultMergePolicy() {
        assertNotNull(instance1);

        CacheSimpleConfig cacheWithDefaultMergePolicyConfig =
                instance1.getConfig().getCacheConfig("cacheWithDefaultMergePolicy");

        assertNotNull(cacheWithDefaultMergePolicyConfig);
        assertEquals(MergePolicyConfig.DEFAULT_MERGE_POLICY,
                cacheWithDefaultMergePolicyConfig.getMergePolicyConfig().getPolicy());
    }

    @Test
    void cacheConfigXmlTest_CustomMergePolicy() {
        assertNotNull(instance1);

        CacheSimpleConfig cacheWithCustomMergePolicyConfig =
                instance1.getConfig().getCacheConfig("cacheWithCustomMergePolicy");

        assertNotNull(cacheWithCustomMergePolicyConfig);
        assertEquals("MyDummyMergePolicy", cacheWithCustomMergePolicyConfig.getMergePolicyConfig().getPolicy());
    }

    @Test
    void cacheConfigXmlTest_ComparatorClassName() {
        assertNotNull(instance1);

        CacheSimpleConfig cacheConfigWithComparatorClassName =
                instance1.getConfig().getCacheConfig("cacheWithComparatorClassName");
        assertNotNull(cacheConfigWithComparatorClassName);

        EvictionConfig evictionConfig = cacheConfigWithComparatorClassName.getEvictionConfig();
        assertNotNull(evictionConfig);

        assertEquals("com.mycompany.MyEvictionPolicyComparator", evictionConfig.getComparatorClassName());
    }

    @Test
    void cacheConfigXmlTest_ComparatorBean() {
        assertNotNull(instance1);

        CacheSimpleConfig cacheConfigWithComparatorClassName =
                instance1.getConfig().getCacheConfig("cacheWithComparatorBean");
        assertNotNull(cacheConfigWithComparatorClassName);

        EvictionConfig evictionConfig = cacheConfigWithComparatorClassName.getEvictionConfig();
        assertNotNull(evictionConfig);

        assertEquals(MyEvictionPolicyComparator.class, evictionConfig.getComparator().getClass());
    }

    @Test
    void cacheConfigXmlTest_SimpleWriterLoader() {
        assertNotNull(instance1);

        CacheSimpleConfig config =
                instance1.getConfig().getCacheConfig("cacheWithSimpleWriterAndLoader");
        assertNotNull(config);

        assertEquals("uk.org.hecaton.config.CacheConfigTest$EmptyCacheWriter", config.getCacheWriter());
        assertEquals("uk.org.hecaton.config.CacheConfigTest$MyCacheLoader", config.getCacheLoader());
    }
}
