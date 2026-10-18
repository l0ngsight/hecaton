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

import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestConfigUtils;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigSmokeTest extends HecatonTestSupport {

    private static final int DEFAULT_INITIAL_CLUSTER_SIZE = 3;

    protected TestHecatonInstanceFactory factory;
    private HecatonInstance[] members;

    @After
    public void tearDown() {
        if (factory != null) {
            factory.terminateAll();
        }
    }

    protected HecatonInstance[] members(int count) {
        return members(count, null);
    }

    protected HecatonInstance[] members(int count, Config config) {
        if (config == null) {
            config = smallInstanceConfig();
        }
        factory = createHecatonInstanceFactory(count);
        members = factory.newInstances(config);
        return members;
    }

    protected HecatonInstance driver() {
        return members[0];
    }

    @Test
    public void multimap_initialSubmitTest() {
        String mapName = randomMapName();

        HecatonInstance[] instances = members(DEFAULT_INITIAL_CLUSTER_SIZE);
        HecatonInstance i1 = driver();

        MultiMapConfig multiMapConfig = new MultiMapConfig(mapName);
        multiMapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = i1.getConfig();
        config.addMultiMapConfig(multiMapConfig);

        for (HecatonInstance instance : instances) {
            multiMapConfig = instance.getConfig().findMultiMapConfig(mapName);
            assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, multiMapConfig.getBackupCount());
        }
    }

    @Test
    public void map_testConflictingDynamicConfig() {
        String mapName = randomMapName();
        int initialClusterSize = 2;

        members(initialClusterSize);
        HecatonInstance driver = driver();

        MapConfig mapConfig1 = new MapConfig(mapName);
        mapConfig1.setBackupCount(0);

        MapConfig mapConfig2 = new MapConfig(mapName);
        mapConfig2.setBackupCount(1);

        driver.getConfig().addMapConfig(mapConfig1);
        assertThrows(InvalidConfigurationException.class, () -> driver.getConfig().addMapConfig(mapConfig2));
    }

    @Test
    public void map_testNonConflictingDynamicConfigWithTheSameName() {
        String mapName = randomMapName();
        members(2);
        HecatonInstance driver = driver();

        MapConfig mapConfig1 = new MapConfig(mapName);
        mapConfig1.setBackupCount(0);

        MapConfig mapConfig2 = new MapConfig(mapName);
        mapConfig2.setBackupCount(0);

        driver.getConfig().addMapConfig(mapConfig1);
        driver.getConfig().addMapConfig(mapConfig2);
    }

    @Test
    public void multimap_withNewMemberJoiningLater() {
        String mapName = randomMapName();

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(DEFAULT_INITIAL_CLUSTER_SIZE);
        HecatonInstance i1 = factory.newHecatonInstance();
        HecatonInstance i2 = factory.newHecatonInstance();

        MultiMapConfig multiMapConfig = new MultiMapConfig(mapName);
        multiMapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = i1.getConfig();
        config.addMultiMapConfig(multiMapConfig);

        // start an instance AFTER the config was already submitted
        HecatonInstance i3 = factory.newHecatonInstance();

        multiMapConfig = i3.getConfig().getMultiMapConfig(mapName);
        assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, multiMapConfig.getBackupCount());
    }

    @Test
    public void map_initialSubmitTest() {
        String mapName = randomMapName();

        HecatonInstance[] instances = members(DEFAULT_INITIAL_CLUSTER_SIZE);
        HecatonInstance driver = driver();

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = driver.getConfig();
        config.addMapConfig(mapConfig);

        for (HecatonInstance instance : instances) {
            mapConfig = instance.getConfig().findMapConfig(mapName);
            assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, mapConfig.getBackupCount());
        }
    }

    @Test
    public void map_initialSubmitTest_withWildcards() {
        String prefixWithWildcard = randomMapName() + "*";

        HecatonInstance[] instances = members(DEFAULT_INITIAL_CLUSTER_SIZE);
        HecatonInstance driver = driver();

        MapConfig mapConfig = new MapConfig(prefixWithWildcard);
        mapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = driver.getConfig();
        config.addMapConfig(mapConfig);

        for (HecatonInstance instance : instances) {
            String randomSuffix = randomMapName();
            mapConfig = instance.getConfig().findMapConfig(prefixWithWildcard + randomSuffix);
            assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, mapConfig.getBackupCount());
        }
    }

    @Test
    public void topic_initialSubmitTest() {
        String topicName = randomName();
        String listenerClassName = randomName();

        HecatonInstance[] instances = members(DEFAULT_INITIAL_CLUSTER_SIZE);
        HecatonInstance driver = driver();

        TopicConfig topicConfig = new TopicConfig(topicName);
        topicConfig.addMessageListenerConfig(new ListenerConfig(listenerClassName));
        driver.getConfig().addTopicConfig(topicConfig);

        for (HecatonInstance instance : instances) {
            topicConfig = instance.getConfig().getTopicConfig(topicName);
            assertEquals(listenerClassName, topicConfig.getMessageListenerConfigs().get(0).getClassName());
        }
    }

    @Test
    public void mapConfig_withLiteMemberJoiningLater_isImmediatelyAvailable() {
        String mapName = randomMapName();

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(DEFAULT_INITIAL_CLUSTER_SIZE);
        HecatonInstance i1 = factory.newHecatonInstance();
        HecatonInstance i2 = factory.newHecatonInstance();

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        Config config = i1.getConfig();
        config.addMapConfig(mapConfig);

        // start a lite member after the dynamic config was submitted
        Config liteConfig = new Config();
        liteConfig.setLiteMember(true);
        HecatonInstance i3 = factory.newHecatonInstance(liteConfig);

        MapConfig mapConfigOnLiteMember = i3.getConfig().getMapConfig(mapName);
        assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, mapConfigOnLiteMember.getBackupCount());
    }

    @Test
    public void map_testNonConflictingStaticConfig() {
        String mapName = "test_map";
        Config config = new Config();
        config.addMapConfig(getMapConfigWithTTL(mapName, 20));

        members(1, config);
        HecatonInstance driver = driver();
        driver.getConfig().addMapConfig(getMapConfigWithTTL(mapName, 20));
    }

    @Test
    public void map_testConflictingStaticConfig() {
        String mapName = "test_map";
        Config config = new Config();
        config.addMapConfig(getMapConfigWithTTL(mapName, 20));

        members(1, config);
        HecatonInstance hz = driver();
        assertThrows(InvalidConfigurationException.class, () -> hz.getConfig().addMapConfig(getMapConfigWithTTL(mapName, 50)));
    }

    @Test
    public void cacheConfig_whenListenerIsRegistered() {
        String cacheName = randomMapName();
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setName(cacheName)
                .setKeyType(String.class.getName())
                .setValueType((new String[0]).getClass().getName())
                .setStatisticsEnabled(false)
                .setManagementEnabled(false);

        members(2);
        HecatonInstance driver = driver();

        driver.getConfig().addCacheConfig(cacheSimpleConfig);
        Cache cache = driver.getCacheManager().getCache(cacheName);
        cache.registerCacheEntryListener(new CacheEntryListenerConfig(() ->
                (CacheEntryCreatedListener & Serializable) System.out::println,
                null,
                true,
                true));
        driver.getConfig().addCacheConfig(cacheSimpleConfig);
    }

    private MapConfig getMapConfigWithTTL(String mapName, int ttl) {
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setTimeToLiveSeconds(ttl);
        return mapConfig;
    }

    public static class CacheEntryListenerConfig extends MutableCacheEntryListenerConfiguration implements Serializable {
        public CacheEntryListenerConfig(Factory listenerFactory, Factory filterFactory, boolean isOldValueRequired,
                                        boolean isSynchronous) {
            super(listenerFactory, filterFactory, isOldValueRequired, isSynchronous);
        }
    }
}
