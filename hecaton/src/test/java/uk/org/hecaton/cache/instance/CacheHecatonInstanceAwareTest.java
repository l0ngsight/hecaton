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

package uk.org.hecaton.cache.instance;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.impl.event.CachePartitionLostEvent;
import uk.org.hecaton.cache.impl.event.CachePartitionLostListener;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.services.NodeAware;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;

/**
 * Test Node & HecatonInstance are injected to {@link NodeAware} and {@link HecatonInstanceAware} cache resources.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheHecatonInstanceAwareTest extends HecatonTestSupport {

    private static final ConcurrentMap<Long, Boolean> HECATON_INSTANCE_INJECTION_RESULT_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Long, Boolean> NODE_INJECTION_RESULT_MAP = new ConcurrentHashMap<>();

    private static final String CACHE_NAME = "MyCache";

    @AfterClass
    public static void destroy() {
        HECATON_INSTANCE_INJECTION_RESULT_MAP.clear();
    }

    protected Config createConfig() {
        return new Config();
    }

    protected CacheConfig<Integer, Integer> createCacheConfig(String cacheName) {
        return new CacheConfig<>(cacheName);
    }

    protected CachingProvider createCachingProvider(HecatonInstance instance) {
        return createServerCachingProvider(instance);
    }

    protected HecatonInstance createInstance() {
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory();
        Config config = createConfig();
        return instanceFactory.newHecatonInstance(config);
    }

    private long generateUniqueHecatonInjectionId() {
        long id;
        for (; ; ) {
            id = System.nanoTime();
            if (HECATON_INSTANCE_INJECTION_RESULT_MAP.putIfAbsent(id, false) == null) {
                break;
            }
        }
        return id;
    }

    @Test
    public void test_injectDependenciesTo_cacheLoader() {
        long id1 = generateUniqueHecatonInjectionId();
        long id2 = generateUniqueHecatonInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setCacheLoaderFactory(new CacheLoaderFactoryWithDependencies(id1, id2));
        cacheConfig.setReadThrough(true);

        HecatonInstance hecatonInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hecatonInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);
        cache.get(2);

        assertEquals("Hecaton instance has not been injected into cache loader factory!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hecaton instance has not been injected into cache loader!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id2));
        assertEquals("Node instance has not been injected into cache loader factory!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Node instance has not been injected into cache loader!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_injectDependenciesTo_cacheWriter() {
        long id1 = generateUniqueHecatonInjectionId();
        long id2 = generateUniqueHecatonInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setCacheWriterFactory(new CacheWriterFactoryWithDependencies(id1, id2));
        cacheConfig.setWriteThrough(true);

        HecatonInstance hecatonInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hecatonInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);

        assertEquals("Hecaton instance has not been injected into cache writer factory!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hecaton instance has not been injected into cache writer!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id2));
        assertEquals("Node instance has not been injected into cache writer factory!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Node instance has not been injected into cache writer!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_injectDependenciesTo_expiryPolicy() {
        long id1 = generateUniqueHecatonInjectionId();
        long id2 = generateUniqueHecatonInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setExpiryPolicyFactory(new ExpiryPolicyFactoryWithDependencies(id1, id2));

        HecatonInstance hecatonInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hecatonInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);

        assertEquals("Hecaton instance has not been injected into expiry policy factory!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hecaton instance has not been injected into expiry policy!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id2));
        assertEquals("Node instance has not been injected into expiry policy factory!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Node instance has not been injected into expiry policy!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_injectDependenciesTo__entryProcessor() {
        long id = generateUniqueHecatonInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HecatonInstance hecatonInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hecatonInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.invoke(1, new EntryProcessorWithDependencies(id));

        assertEquals("Hecaton instance has not been injected into entry processor!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id));

        assertEquals("Node instance has not been injected into entry processor!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id));
    }

    @Test
    public void test_injectDependenciesTo_completionListener() {
        final long id = generateUniqueHecatonInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HecatonInstance hecatonInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hecatonInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.loadAll(new HashSet<>(), false, new CompletionListenerWithDependencies(id));

        assertEqualsEventually(() -> HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id), Boolean.TRUE);
        // Node is only injected on member side completion listener
        if (!ClassLoaderUtil.isClassAvailable(null, "uk.org.hecaton.client.HecatonClient")) {
            assertEqualsEventually(() -> NODE_INJECTION_RESULT_MAP.get(id), Boolean.TRUE);
        }
    }

    @Test
    public void test_injectDependenciesTo__entryListener() {
        long id1 = generateUniqueHecatonInjectionId();
        long id2 = generateUniqueHecatonInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.addCacheEntryListenerConfiguration(
                new HecatonInstanceAwareCacheEntryListenerConfiguration(id1, id2));

        HecatonInstance hecatonInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hecatonInstance).getCacheManager();
        cacheManager.createCache(CACHE_NAME, cacheConfig);

        assertEquals("Hecaton instance has not been injected into entry listener factory!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hecaton instance has not been injected into entry listener!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id2));
        // Node is only injected on member side entry listeners
        if (!ClassLoaderUtil.isClassAvailable(null, "uk.org.hecaton.client.HecatonClient")) {
            assertEquals("Node instance has not been injected into entry listener factory!", Boolean.TRUE,
                    NODE_INJECTION_RESULT_MAP.get(id1));
            assertEquals("Node instance has not been injected into entry listener!", Boolean.TRUE,
                    NODE_INJECTION_RESULT_MAP.get(id2));
        }
    }

    @Test
    public void test_injectDependenciesTo_partitionLostListener() {
        long id = generateUniqueHecatonInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HecatonInstance hecatonInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hecatonInstance).getCacheManager();
        ICache<Integer, Integer> cache = (ICache<Integer, Integer>) cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.addPartitionLostListener(new CachePartitionLostListenerWithDependencies(id));

        assertEquals("Hecaton instance has not been injected into partition lost listener!",
                Boolean.TRUE, HECATON_INSTANCE_INJECTION_RESULT_MAP.get(id));
        // Node is only injected on member side listeners
        if (!ClassLoaderUtil.isClassAvailable(null, "uk.org.hecaton.client.HecatonClient")) {
            assertEquals("Node instance has not been injected into partition lost listener!", Boolean.TRUE,
                    NODE_INJECTION_RESULT_MAP.get(id));
        }
    }

    public static class CacheLoaderFactoryWithDependencies
            implements Factory<CacheLoader<Integer, Integer>>, HecatonInstanceAware, NodeAware {

        private final long id1;
        private final long id2;

        CacheLoaderFactoryWithDependencies(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheLoader<Integer, Integer> create() {
            return new CacheLoaderWithDependencies(id2);
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class CacheLoaderWithDependencies implements CacheLoader<Integer, Integer>, HecatonInstanceAware, NodeAware {

        private final long id2;

        CacheLoaderWithDependencies(long id2) {
            this.id2 = id2;
        }

        @Override
        public Integer load(Integer key) throws CacheLoaderException {
            return null;
        }

        @Override
        public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys)
                throws CacheLoaderException {
            return null;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id2, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id2, true);
        }
    }

    public static class CacheWriterFactoryWithDependencies
            implements Factory<CacheWriter<Integer, Integer>>, HecatonInstanceAware, NodeAware {

        private final long id1;
        private final long id2;

        CacheWriterFactoryWithDependencies(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheWriter<Integer, Integer> create() {
            return new CacheWriterWithDependencies(id2);
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class CacheWriterWithDependencies
            implements CacheWriter<Integer, Integer>, HecatonInstanceAware, NodeAware {

        private final long id;

        CacheWriterWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void write(Cache.Entry<? extends Integer, ? extends Integer> entry)
                throws CacheWriterException {
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries)
                throws CacheWriterException {
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id, true);
        }
    }

    public static class ExpiryPolicyFactoryWithDependencies implements Factory<ExpiryPolicy>, HecatonInstanceAware,
            NodeAware {

        private final long id1;
        private final long id2;

        ExpiryPolicyFactoryWithDependencies(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public ExpiryPolicy create() {
            return new ExpiryPolicyWithDependencies(id2);
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class ExpiryPolicyWithDependencies implements ExpiryPolicy, HecatonInstanceAware, NodeAware {

        private final long id;

        ExpiryPolicyWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public Duration getExpiryForCreation() {
            return Duration.ETERNAL;
        }

        @Override
        public Duration getExpiryForAccess() {
            return Duration.ETERNAL;
        }

        @Override
        public Duration getExpiryForUpdate() {
            return Duration.ETERNAL;
        }
    }

    public static class EntryProcessorWithDependencies
            implements EntryProcessor<Integer, Integer, Integer>, HecatonInstanceAware, NodeAware, Serializable {

        private final long id;

        EntryProcessorWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
                throws EntryProcessorException {
            return null;
        }
    }

    public static class CompletionListenerWithDependencies implements CompletionListener, HecatonInstanceAware, NodeAware {

        private final long id;

        CompletionListenerWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void onCompletion() {
        }

        @Override
        public void onException(Exception e) {
        }
    }

    public static class HecatonInstanceAwareCacheEntryListenerConfiguration
            implements CacheEntryListenerConfiguration<Integer, Integer> {

        private final long id1;
        private final long id2;

        HecatonInstanceAwareCacheEntryListenerConfiguration(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public Factory<CacheEntryListener<? super Integer, ? super Integer>> getCacheEntryListenerFactory() {
            return new CacheEntryListenerFactoryWithDependencies(id1, id2);
        }

        @Override
        public boolean isOldValueRequired() {
            return false;
        }

        @Override
        public Factory<CacheEntryEventFilter<? super Integer, ? super Integer>> getCacheEntryEventFilterFactory() {
            return null;
        }

        @Override
        public boolean isSynchronous() {
            return false;
        }
    }

    public static class CacheEntryListenerFactoryWithDependencies
            implements Factory<CacheEntryListener<? super Integer, ? super Integer>>, HecatonInstanceAware,
            NodeAware, Serializable {

        private final long id1;
        private final long id2;

        CacheEntryListenerFactoryWithDependencies(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheEntryListener<? super Integer, ? super Integer> create() {
            return new CacheEntryListenerWithDependencies(id2);
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.putIfAbsent(id1, true);
        }
    }

    public static class CacheEntryListenerWithDependencies
            implements CacheEntryListener<Integer, Integer>, HecatonInstanceAware, NodeAware, Serializable {

        private final long id;

        CacheEntryListenerWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.putIfAbsent(id, true);
        }
    }

    public static class CachePartitionLostListenerWithDependencies
            implements CachePartitionLostListener, HecatonInstanceAware, NodeAware, Serializable {

        private final long id;

        CachePartitionLostListenerWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            HECATON_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.putIfAbsent(id, true);
        }

        @Override
        public void partitionLost(CachePartitionLostEvent event) {
        }
    }
}
