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

import uk.org.hecaton.cache.CachePartitionLostListenerTest.EventCollectingCachePartitionLostListener;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.HecatonServerCachingProvider;
import uk.org.hecaton.cache.impl.event.CachePartitionLostListener;
import uk.org.hecaton.config.CachePartitionLostListenerConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.config.CachePartitionLostListenerConfigReadOnly;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.EventListener;
import java.util.List;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CachePartitionLostListenerConfigTest extends HecatonTestSupport {

    private final URL configUrl = getClass().getClassLoader().getResource("test-hecaton-jcache-partition-lost-listener.xml");

    @Test
    public void testCachePartitionLostListener_registeredViaImplementationInConfigObject() {
        final String cacheName = "myCache";

        Config config = new Config();
        CacheSimpleConfig cacheConfig = config.getCacheConfig(cacheName);
        CachePartitionLostListener listener = mock(CachePartitionLostListener.class);
        cacheConfig.addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig(listener));
        cacheConfig.setBackupCount(0);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance instance = factory.newHecatonInstance(config);
        HecatonServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.getCache(cacheName);

        final EventService eventService = getNode(instance).getNodeEngine().getEventService();

        assertTrueEventually(() -> {
            Collection<EventRegistration> registrations = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);
            assertFalse(registrations.isEmpty());
        });
    }

    @Test
    public void cacheConfigXmlTest() throws IOException {
        String cacheName = "cacheWithPartitionLostListener";
        Config config = new XmlConfigBuilder(configUrl).build();
        CacheSimpleConfig cacheConfig = config.getCacheConfig(cacheName);

        List<CachePartitionLostListenerConfig> configs = cacheConfig.getPartitionLostListenerConfigs();
        assertEquals(1, configs.size());
        assertEquals("DummyCachePartitionLostListenerImpl", configs.get(0).getClassName());
    }

    @Test
    public void testCachePartitionLostListenerConfig_setImplementation() {
        CachePartitionLostListener listener = mock(CachePartitionLostListener.class);
        CachePartitionLostListenerConfig listenerConfig = new CachePartitionLostListenerConfig();
        listenerConfig.setImplementation(listener);

        assertEquals(listener, listenerConfig.getImplementation());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCachePartitionLostListenerReadOnlyConfig_withClassName() {
        CachePartitionLostListenerConfigReadOnly readOnly
                = new CachePartitionLostListenerConfigReadOnly(new CachePartitionLostListenerConfig());
        readOnly.setClassName("com.hz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCachePartitionLostListenerReadOnlyConfig_withImplementation() {
        CachePartitionLostListener listener = mock(CachePartitionLostListener.class);
        CachePartitionLostListenerConfig listenerConfig = new CachePartitionLostListenerConfig(listener);
        CachePartitionLostListenerConfigReadOnly readOnly = new CachePartitionLostListenerConfigReadOnly(listenerConfig);
        assertEquals(listener, readOnly.getImplementation());
        readOnly.setImplementation(mock(CachePartitionLostListener.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCachePartitionLostListenerReadOnlyConfig_withEventListenerImplementation() {
        CachePartitionLostListenerConfigReadOnly readOnly
                = new CachePartitionLostListenerConfigReadOnly(new CachePartitionLostListenerConfig());
        readOnly.setImplementation(mock(EventListener.class));
    }

    @Test
    public void testGetImplementation() {
        CachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(0);
        CachePartitionLostListenerConfig listenerConfig = new CachePartitionLostListenerConfig(listener);
        assertEquals(listener, listenerConfig.getImplementation());
    }

    @Test
    public void testGetClassName() {
        String className = "EventCollectingCachePartitionLostListener";
        CachePartitionLostListenerConfig listenerConfig = new CachePartitionLostListenerConfig(className);
        assertEquals(className, listenerConfig.getClassName());
    }

    @Test
    public void testEqualsAndHashCode() {
        CachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(0);

        CachePartitionLostListenerConfig listenerConfig1 = new CachePartitionLostListenerConfig();
        CachePartitionLostListenerConfig listenerConfig2 = new CachePartitionLostListenerConfig();

        assertEquals(listenerConfig1, listenerConfig1);
        assertEquals(new CachePartitionLostListenerConfig(), listenerConfig1);
        assertNotEquals(null, listenerConfig1);
        assertNotEquals(new Object(), listenerConfig1);

        listenerConfig1.setImplementation(listener);
        assertNotEquals(listenerConfig1, listenerConfig2);
        assertNotEquals(listenerConfig1.hashCode(), listenerConfig2.hashCode());

        listenerConfig2.setImplementation(listener);
        assertEquals(listenerConfig1, listenerConfig2);
        assertEquals(listenerConfig1.hashCode(), listenerConfig2.hashCode());

        listenerConfig1.setClassName("EventCollectingCachePartitionLostListener");
        listenerConfig2.setClassName("CachePartitionLostListenerConfig");
        assertNotEquals(listenerConfig1, listenerConfig2);
        assertNotEquals(listenerConfig1.hashCode(), listenerConfig2.hashCode());

        listenerConfig2.setClassName("EventCollectingCachePartitionLostListener");
        assertEquals(listenerConfig1, listenerConfig2);
        assertEquals(listenerConfig1.hashCode(), listenerConfig2.hashCode());
    }
}
