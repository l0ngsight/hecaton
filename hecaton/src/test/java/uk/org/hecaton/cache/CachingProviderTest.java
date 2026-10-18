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

import uk.org.hecaton.cache.impl.AbstractHecatonCachingProvider;
import uk.org.hecaton.config.ClasspathXmlConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.cache.HecatonCachingProvider.propertiesByInstanceItself;
import static uk.org.hecaton.cache.HecatonCachingProvider.propertiesByInstanceName;
import static uk.org.hecaton.cache.HecatonCachingProvider.propertiesByLocation;
import static uk.org.hecaton.cache.jsr.JsrTestUtil.cleanup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class CachingProviderTest extends HecatonTestSupport {

    protected static final int INSTANCE_COUNT = 3;
    protected static final String INSTANCE_1_NAME = randomString();
    protected static final String INSTANCE_2_NAME = randomString();
    protected static final String CONFIG_CLASSPATH_LOCATION = "test-hecaton-jcache.xml";

    protected TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(INSTANCE_COUNT);
    protected HecatonInstance instance1;
    protected HecatonInstance instance2;
    protected HecatonInstance instance3;
    protected CachingProvider cachingProvider;

    @Before
    public void setup() {
        instance1 = createHecatonInstance(INSTANCE_1_NAME);
        instance2 = createHecatonInstance(INSTANCE_2_NAME);
        cachingProvider = createCachingProvider(instance1);
        // also start a hecaton instance off a well-known config location
        Config config = new ClasspathXmlConfig(CONFIG_CLASSPATH_LOCATION);
        instance3 = instanceFactory.newHecatonInstance(config);
    }

    protected String getConfigClasspathLocation() {
        return CONFIG_CLASSPATH_LOCATION;
    }

    protected String getProviderType() {
        return "server";
    }

    protected HecatonInstance createHecatonInstance(String instanceName) {
        Config config = new Config();
        config.setInstanceName(instanceName);
        config.setClusterName("test-cluster1");
        return instanceFactory.newHecatonInstance(config);
    }

    protected CachingProvider createCachingProvider(HecatonInstance defaultInstance) {
        return createServerCachingProvider(defaultInstance);
    }

    @Test
    public void whenDefaultURI_instanceNameAsProperty_thenThatInstanceIsUsed() {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                null, null, propertiesByInstanceName(INSTANCE_2_NAME));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenOtherURI_instanceNameAsProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                new URI("other-uri"), null, propertiesByInstanceName(INSTANCE_2_NAME));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenDefaultURI_inexistentInstanceNameAsProperty_thenStartsOtherInstance() {
        cachingProvider.getCacheManager(null, null, propertiesByInstanceName("instance-does-not-exist"));
        assertInstanceStarted("instance-does-not-exist");
    }

    @Test
    public void whenOtherURI_inexistentInstanceNameAsProperty_thenStartsNewInstance() throws URISyntaxException {
        cachingProvider.getCacheManager(new URI("other-uri"), null, propertiesByInstanceName("instance-does-not-exist"));
        assertInstanceStarted("instance-does-not-exist");
    }

    @Test
    public void whenDefaultURI_noInstanceName_thenUseDefaultHecatonInstance() {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager();
        assertCacheManagerInstance(cacheManager, instance1);
    }

    @Test
    public void whenConfigLocationAsUri_thenThatInstanceIsUsed() throws URISyntaxException {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:" + getConfigClasspathLocation()), null);
        assertCacheManagerInstance(cacheManager, instance3);
    }

    @Test
    public void whenConfigLocationAsUriViaProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        System.setProperty("PROPERTY_PLACEHOLDER", "classpath:" + getConfigClasspathLocation());
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                new URI("PROPERTY_PLACEHOLDER"), null);
        assertCacheManagerInstance(cacheManager, instance3);
    }

    @Test(expected = CacheException.class)
    public void whenInvalidConfigLocationAsUri_thenFails() throws URISyntaxException {
        cachingProvider.getCacheManager(new URI("classpath:this-config-does-not-exist"), null);
    }

    @Test
    public void whenConfigLocationAsProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:this-config-does-not-exist"), null,
                propertiesByLocation("classpath:" + getConfigClasspathLocation()));
        assertCacheManagerInstance(cacheManager, instance3);
    }

    // test that instance property has priority over attempting URI interpretation:
    // CacheManager's URI points to invalid config location, however a valid instance name is
    // specified in properties, so this instance is picked up.
    @Test
    public void whenInstanceNameAsProperty_thenThatInstanceIsUsed() throws URISyntaxException {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:this-config-does-not-exist"), null, propertiesByInstanceName(INSTANCE_2_NAME));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenInstanceItselfAsProperty_andInvalidConfigURI_thenInstanceItselfIsUsed() throws URISyntaxException {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:this-config-does-not-exist"), null, propertiesByInstanceItself(instance2));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenInstanceItselfAsProperty_andValidConfigURI_thenInstanceItselfIsUsed() throws URISyntaxException {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                new URI("classpath:" + getConfigClasspathLocation()), null, propertiesByInstanceItself(instance2));
        assertCacheManagerInstance(cacheManager, instance2);
    }

    @Test
    public void whenInstanceItselfAsProperty_andDefaultURI_thenInstanceItselfIsUsed() {
        HecatonCacheManager cacheManager = (HecatonCacheManager) cachingProvider.getCacheManager(
                null, null, propertiesByInstanceItself(instance3));
        assertCacheManagerInstance(cacheManager, instance3);
    }

    @Test
    public void whenDefaultCacheManager_withUnnamedDefaultInstance_thenNoSharedNameHecatonInstanceExists() {
        cleanupForDefaultCacheManagerTest();
        try {
            System.setProperty(AbstractHecatonCachingProvider.NAMED_JCACHE_HZ_INSTANCE, "false");
            System.setProperty(ClusterProperty.JCACHE_PROVIDER_TYPE.getName(), getProviderType());
            CachingProvider defaultCachingProvider = Caching.getCachingProvider();
            CacheManager defaultCacheManager = defaultCachingProvider.getCacheManager();
            Collection<HecatonInstance> instances = getStartedInstances();
            for (HecatonInstance instance : instances) {
                if (HecatonCachingProvider.SHARED_JCACHE_INSTANCE_NAME.equals(instance.getName())) {
                    fail("The default named HecatonInstance shouldn't have been started");
                }
            }
            defaultCachingProvider.close();
        } finally {
            cleanup();
        }
    }

    @Test
    public void whenDefaultCacheManager_thenSharedNameHecatonInstanceExists() {
        cleanupForDefaultCacheManagerTest();
        try {
            System.setProperty(ClusterProperty.JCACHE_PROVIDER_TYPE.getName(), getProviderType());
            CachingProvider defaultCachingProvider = Caching.getCachingProvider();
            CacheManager defaultCacheManager = defaultCachingProvider.getCacheManager();
            Collection<HecatonInstance> instances = getStartedInstances();
            boolean sharedInstanceStarted = false;
            for (HecatonInstance instance : instances) {
                if (instance.getName().equals(HecatonCachingProvider.SHARED_JCACHE_INSTANCE_NAME)) {
                    sharedInstanceStarted = true;
                }
            }
            assertTrue("The default named HecatonInstance should have been started", sharedInstanceStarted);
            defaultCachingProvider.close();
        } finally {
            cleanup();
        }
    }

    private void assertCacheManagerInstance(HecatonCacheManager cacheManager, HecatonInstance instance) {
        assertEquals(instance, cacheManager.getHecatonInstance());
    }

    protected void assertInstanceStarted(String instanceName) {
        HecatonInstance otherInstance = Hecaton.getHecatonInstanceByName(instanceName);
        assertNotNull(otherInstance);
        otherInstance.getLifecycleService().terminate();
    }

    protected Collection<HecatonInstance> getStartedInstances() {
        return Hecaton.getAllHecatonInstances();
    }

    // tests for the default cache manager require cleanup before running which must be overridden for client-side tests
    protected void cleanupForDefaultCacheManagerTest() {
        cleanup();
    }
}
