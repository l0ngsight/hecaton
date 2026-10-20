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

package uk.org.hecaton.cache;

import classloading.domain.Person;
import classloading.domain.PersonCacheLoaderFactory;
import classloading.domain.PersonEntryProcessor;
import classloading.domain.PersonExpiryPolicyFactory;
import uk.org.hecaton.cache.impl.CacheProxy;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.UserCodeDeploymentConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.FilteringClassLoader;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.List;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.cache.HecatonCachingProvider.propertiesByInstanceItself;
import static uk.org.hecaton.config.UserCodeDeploymentConfig.ClassCacheMode.OFF;
import static uk.org.hecaton.internal.util.RootCauseMatcher.rootCause;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

/**
 * Test transfer of CacheConfig's of typed Caches to a joining member
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheTypesConfigTest extends HecatonTestSupport {

    protected String cacheName;
    protected TestHecatonInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHecatonInstanceFactory();
        cacheName = randomName();
    }

    // Even when the key or value class is not present in the classpath of the target member, a joining member will join.
    // Some Cache operations that require deserialization on the member will fail (eg entry processors)
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenClassNotResolvable() {
        // create a HecatonInstance with a CacheConfig referring to a Class not resolvable on the joining member
        HecatonInstance hz1 = factory.newHecatonInstance(getConfig());
        CachingProvider cachingProvider = createServerCachingProvider(hz1);
        cachingProvider.getCacheManager().createCache(cacheName, createCacheConfig());

        // joining member cannot resolve Person class
        HecatonInstance hz2 = factory.newHecatonInstance(getClassFilteringConfig());
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> cache = hz1.getCacheManager().getCache(cacheName);
        String key = generateKeyOwnedBy(hz2);
        // a simple put will work as hz2 will just receive a Data value blob
        cache.put(key, new Person());

        assertThatThrownBy(() -> cache.invoke(key, new PersonEntryProcessor()))
                .has(rootCause(ClassNotFoundException.class, "classloading.domain.PersonEntryProcessor - "
                        + "Package excluded explicitly"));
    }

    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenNoMemberResolvesClass() {
        // given a member who cannot resolve the value type of a CacheConfig
        HecatonInstance hz1 = factory.newHecatonInstance(getClassFilteringConfig());
        // and a new member that creates a CacheConfig with an explicit value type
        HecatonInstance hz2 = factory.newHecatonInstance(getConfig());
        CachingProvider cachingProvider = createServerCachingProvider(hz1);
        cachingProvider.getCacheManager().createCache(cacheName, createCacheConfig());
        // ensure cluster is formed but cache is not used
        assertClusterSize(2, hz1, hz2);
        // member that is aware of value type leaves cluster
        hz2.shutdown();

        // then new member unaware of the value type can join the cluster
        hz2 = factory.newHecatonInstance(getClassFilteringConfig());
        assertClusterSize(2, hz1, hz2);
    }

    // When the joining member is not aware of key or value class but it is later resolvable via user code deployment, then
    // all Cache features should work
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenKVTypesAvailableViaUserCodeDeployment() {
        // create a HecatonInstance with a CacheConfig referring to a Class resolvable via user code deployment
        Config config = getConfig();
        UserCodeDeploymentConfig codeDeploymentConfig = new UserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(OFF)
                .setWhitelistedPrefixes("classloading");
        config.setUserCodeDeploymentConfig(codeDeploymentConfig);
        HecatonInstance hz1 = factory.newHecatonInstance(config);
        CachingProvider cachingProvider = createServerCachingProvider(hz1);
        cachingProvider.getCacheManager().createCache(cacheName, createCacheConfig());

        // joining member cannot resolve Person class but it is resolvable on other member via user code deployment
        Config joiningMemberConfig = getClassFilteringConfig();
        joiningMemberConfig.setUserCodeDeploymentConfig(codeDeploymentConfig);
        HecatonInstance hz2 = factory.newHecatonInstance(joiningMemberConfig);
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> testCache = hz1.getCacheManager().getCache(cacheName);
        String key = generateKeyOwnedBy(hz2);
        testCache.put(key, new Person());
        testCache.invoke(key, new PersonEntryProcessor());
        assertNotNull(testCache.get(key));
    }

    // tests deferred resolution of factories
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenCacheLoaderFactoryNotResolvable() {
        HecatonInstance hz1 = factory.newHecatonInstance(getConfig());
        CachingProvider cachingProvider = createServerCachingProvider(hz1);
        CacheManager cacheManager1 = cachingProvider.getCacheManager(null, null, propertiesByInstanceItself(hz1));
        CacheConfig cacheConfig = createCacheConfig();
        cacheConfig.setCacheLoaderFactory(new PersonCacheLoaderFactory());
        cacheManager1.createCache(cacheName, cacheConfig);

        // joining member cannot resolve PersonCacheLoaderFactory class
        HecatonInstance hz2 = factory.newHecatonInstance(getClassFilteringConfig());
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> cache = hz2.getCacheManager().getCache(cacheName);
        String key = generateKeyOwnedBy(hz2);
        assertThatThrownBy(() -> cache.invoke(key, new PersonEntryProcessor()))
                .has(rootCause(ClassNotFoundException.class, "classloading.domain.PersonCacheLoaderFactory - "
                        + "Package excluded explicitly"));
    }

    // tests deferred resolution of factories, with context class loader set correctly
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenCacheLoaderFactoryNotResolvableWithClassLoaderSet() {
        HecatonInstance hz1 = factory.newHecatonInstance(getConfig());
        CachingProvider cachingProvider = createServerCachingProvider(hz1);
        CacheManager cacheManager1 = cachingProvider.getCacheManager(null, null, propertiesByInstanceItself(hz1));
        CacheConfig<String, Person> cacheConfig = createCacheConfig();
        cacheConfig.setCacheLoaderFactory(new PersonCacheLoaderFactory());
        cacheManager1.createCache(cacheName, cacheConfig);

        // joining member cannot resolve PersonCacheLoaderFactory class
        HecatonInstance hz2 = factory.newHecatonInstance(getClassFilteringConfig());
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> cache = hz2.getCacheManager().getCache(cacheName);
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(hz2.getConfig().getClassLoader());
            CacheProxy<String, Person> cacheProxy = (CacheProxy<String, Person>) cache;
            CacheService cacheService = (CacheService) cacheProxy.getService();
            assertThatThrownBy(() -> {
                cacheService.getCacheConfig(cache.getPrefixedName()).getCacheLoaderFactory();
                String key = generateKeyOwnedBy(hz2);
                cache.invoke(key, new PersonEntryProcessor());
            }).has(rootCause(ClassNotFoundException.class, "classloading.domain.PersonCacheLoaderFactory - "
                    + "Package excluded explicitly"));
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    // tests deferred resolution of expiry policy factory
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenExpiryPolicyFactoryNotResolvable() {
        HecatonInstance hz1 = factory.newHecatonInstance(getConfig());
        CachingProvider cachingProvider = createServerCachingProvider(hz1);
        cachingProvider.getCacheManager().createCache(cacheName, createCacheConfig()
                .setExpiryPolicyFactory(new PersonExpiryPolicyFactory()));

        HecatonInstance hz2 = factory.newHecatonInstance(getClassFilteringConfig());
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> cache = hz2.getCacheManager().getCache(cacheName);
        String key = generateKeyOwnedBy(hz2);
        assertThatThrownBy(() -> cache.invoke(key, new PersonEntryProcessor()))
                .has(rootCause(ClassNotFoundException.class, "classloading.domain.PersonExpiryPolicyFactory - "
                        + "Package excluded explicitly"));
    }

    // overridden in another context
    CacheConfig<String, Person> createCacheConfig() {
        CacheConfig<String, Person> cacheConfig = new CacheConfig<>();
        cacheConfig.setTypes(String.class, Person.class);
        return cacheConfig;
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig()
                .setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "10");
    }

    private Config getClassFilteringConfig() {
        List<String> excludes = Arrays.asList("classloading");
        ClassLoader classLoader = new FilteringClassLoader(excludes, null);
        return getConfig().setClassLoader(classLoader);
    }
}
