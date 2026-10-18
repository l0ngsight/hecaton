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

package uk.org.hecaton.spring.cache;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.spring.CustomSpringExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.cache.Cache;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"jCacheCacheManager-applicationContext-DI.xml"})
class JCacheCacheManagerDITest {

    @Autowired
    private JCacheCacheManager springCacheManager;

    @BeforeAll
    @AfterAll
    public static void start() {
        Hecaton.shutdownAll();
    }

    @Test
    void testCacheWithCacheLoaderFactory_dependenciesInjected() {
        Cache<Integer, ?> cacheWithLoader = springCacheManager.getCacheManager().getCache("cacheWithLoader");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithLoader.get(1);

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue(JCacheCacheLoaderFactory.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCacheCacheLoaderFactory.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheLoaderFactory.INJECTED_DUMMY_BEAN.get(), "Spring bean not injected to @SpringAware object");
        assertTrue(JCacheCacheLoader.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCacheCacheLoader.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheLoader.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
    }

    @Test
    void testCacheWithCacheWriterFactory_dependenciesInjected() {
        Cache<Integer, String> cacheWithWriter = springCacheManager.getCacheManager().getCache("cacheWithWriter");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithWriter.put(1, "1");

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue(JCacheCacheWriterFactory.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCacheCacheWriterFactory.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheWriterFactory.INJECTED_DUMMY_BEAN.get(), "Spring bean not injected to @SpringAware object");
        assertTrue(JCacheCacheWriter.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCacheCacheWriter.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheWriter.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
    }

    @Test
    void testCacheWithExpiryPolicyFactory_dependenciesInjected() {
        Cache<Integer, String> cacheWithExpiryPolicy = springCacheManager.getCacheManager().getCache("cacheWithExpiryPolicy");
        // use cacheloader at least once to ensure it's instantiated
        cacheWithExpiryPolicy.put(1, "1");

        // ensure CacheLoaderFactory & CacheLoader dependencies have been injected
        assertTrue(JCacheExpiryPolicyFactory.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCacheExpiryPolicyFactory.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheExpiryPolicyFactory.INJECTED_DUMMY_BEAN.get(), "Spring bean not injected to @SpringAware object");
        assertTrue(JCacheExpiryPolicy.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCacheExpiryPolicy.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheExpiryPolicy.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
    }

    @Test
    void testCacheWithListeners_dependenciesInjected() {
        Cache<Integer, ?> cacheWithPartitionLostListener = springCacheManager.getCacheManager()
                .getCache("cacheWithListeners");

        cacheWithPartitionLostListener.get(1);

        // ensure partition lost listener got its dependencies injected
        assertTrue(JCachePartitionLostListener.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCachePartitionLostListener.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCachePartitionLostListener.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
        // ensure CacheEntryListenerFactory & CacheEntryListener dependencies have been injected
        assertTrue(JCacheCacheEntryListenerFactory.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCacheCacheEntryListenerFactory.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheEntryListenerFactory.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
        assertTrue(JCacheCacheEntryListener.HECATON_INSTANCE_INJECTED.get(), "HecatonInstance not injected to HecatonInstanceAware object");
        assertTrue(JCacheCacheEntryListener.NODE_INJECTED.get(), "Node not injected to NodeAware object");
        assertNotNull(JCacheCacheEntryListener.instance.getDummyBean(), "Spring bean not injected to @SpringAware object");
    }

}
