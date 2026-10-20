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

import uk.org.hecaton.cache.HecatonCacheManager;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MerkleTreeConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.cache.CacheUtil.getDistributedObjectName;
import static uk.org.hecaton.cache.HecatonCachingProvider.propertiesByInstanceItself;
import static uk.org.hecaton.cache.impl.ICacheService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

// asserts contents of AbstractCacheService.configs
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheConfigPropagationTest extends HecatonTestSupport {

    private static final String DYNAMIC_CACHE_NAME = "dynamic-cache";
    private static final String DECLARED_CACHE_NAME = "declared-cache-1";

    protected int clusterSize = 2;
    protected HecatonInstance driver;
    protected TestHecatonInstanceFactory factory;

    private CacheManager cacheManagerDriver;
    private HecatonInstance[] members;

    @Before
    public void setup() {
        setupFactory();
        members = createMembers();
        driver = createTestDriver();
        cacheManagerDriver = createCacheManagerTestDriver();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void noPreJoinCacheConfig_whenCacheCreatedDynamically_viaCacheManager() {
        cacheManagerDriver.createCache(DYNAMIC_CACHE_NAME, new CacheConfig<String, String>());
        CacheService cacheService = getCacheService(members[0]);
        assertNoPreJoinCacheConfig(cacheService);
    }

    @Test
    public void noPreJoinCacheConfig_whenCacheGet_viaCacheManager() {
        cacheManagerDriver.getCache(DECLARED_CACHE_NAME);
        CacheService cacheService = getCacheService(members[0]);
        assertNoPreJoinCacheConfig(cacheService);
        assertNotNull("Cache config 'declared-cache-1' should exist in registered cache configs",
                cacheService.getCacheConfig(getDistributedObjectName(DECLARED_CACHE_NAME, null, null)));
    }

    @Test
    public void noPreJoinCacheConfig_whenCacheGet_viaHecatonInstance() {
        driver.getCacheManager().getCache(DECLARED_CACHE_NAME);
        CacheService cacheService = getCacheService(members[0]);
        assertNoPreJoinCacheConfig(cacheService);
    }

    @Test
    public void noPreJoinCacheConfig_onNewMember() {
        cacheManagerDriver.createCache(DYNAMIC_CACHE_NAME, new CacheConfig<String, String>());
        HecatonInstance newMember = factory.newHecatonInstance(getConfig());
        CacheService cacheService = getCacheService(newMember);
        assertNoPreJoinCacheConfig(cacheService);
    }

    @Test
    public void cacheConfig_existsOnRemoteMember_immediatelyAfterCacheGet() {
        CacheService cacheServiceOnRemote = getCacheService(members[1]);
        String distributedObjectName = getDistributedObjectName(DECLARED_CACHE_NAME);
        assertNull(cacheServiceOnRemote.getCacheConfig(distributedObjectName));
        driver.getCacheManager().getCache(DECLARED_CACHE_NAME);
        assertNotNull(cacheServiceOnRemote.getCacheConfig(distributedObjectName));
    }

    @Test
    public void server_receives_same_merkle_tree_config_given_from_client() {
        MerkleTreeConfig expectedMerkleTreeConfig = new MerkleTreeConfig()
                .setDepth(11).setEnabled(true);

        CacheConfig<String, String> cacheConfig = new CacheConfig<>();
        cacheConfig.setMerkleTreeConfig(expectedMerkleTreeConfig);
        cacheManagerDriver.createCache(DYNAMIC_CACHE_NAME, cacheConfig);
        CacheService cacheService = getCacheService(members[0]);

        ConcurrentMap<String, CacheConfig> configs = cacheService.getConfigs();
        CacheConfig actualConfig = configs.get(HecatonCacheManager.CACHE_MANAGER_PREFIX + DYNAMIC_CACHE_NAME);

        assertEquals(expectedMerkleTreeConfig, actualConfig.getMerkleTreeConfig());
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.addCacheConfig(new CacheSimpleConfig()
                .setName("declared-cache*"));
        return config;
    }

    protected void setupFactory() {
        factory = createHecatonInstanceFactory();
    }

    protected HecatonInstance createTestDriver() {
        return factory.newHecatonInstance(getConfig());
    }

    protected CacheManager createCacheManagerTestDriver() {
        return createServerCachingProvider(driver).getCacheManager(null, null,
                propertiesByInstanceItself(driver));
    }

    protected HecatonInstance[] createMembers() {
        return factory.newInstances(getConfig(), clusterSize);
    }

    protected HecatonInstance newMember() {
        return factory.newHecatonInstance(getConfig());
    }

    protected CacheService getCacheService(HecatonInstance member) {
        return getNodeEngineImpl(member).getService(SERVICE_NAME);
    }

    private void assertNoPreJoinCacheConfig(CacheService cacheService) {
        for (CacheConfig cacheConfig : cacheService.getCacheConfigs()) {
            Assert.assertFalse("No PreJoinCacheConfig should exist in CacheService configs",
                    cacheConfig instanceof PreJoinCacheConfig);
        }
    }
}
