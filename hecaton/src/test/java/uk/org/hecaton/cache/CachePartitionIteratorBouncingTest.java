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

import uk.org.hecaton.cache.impl.CacheProxy;
import uk.org.hecaton.client.cache.impl.ClientCacheProxy;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.bounce.BounceMemberRule;
import uk.org.hecaton.test.bounce.BounceTestConfiguration.DriverType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.Cache.Entry;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static uk.org.hecaton.config.EvictionConfig.DEFAULT_MAX_SIZE_POLICY;
import static uk.org.hecaton.config.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class CachePartitionIteratorBouncingTest extends HecatonTestSupport {

    private final Logger logger = Logger.getLogger(getClass().getName());
    private static final String TEST_CACHE_NAME = "testCache";
    private static final int STABLE_ENTRY_COUNT = 10000;
    private static final int CONCURRENCY = 2;
    public static final int FETCH_SIZE = 100;
    public static final int MUTATION_ENTRY_FACTOR = 10;
    public AtomicInteger successfullIterations = new AtomicInteger();

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(getConfig())
                            .clusterSize(4)
                            .driverCount(4)
                            .driverType(isClientDriver() ? DriverType.CLIENT : DriverType.MEMBER)
                            .build();

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        MaxSizePolicy maxSizePolicy = getInMemoryFormat() == InMemoryFormat.NATIVE
                ? USED_NATIVE_MEMORY_SIZE
                : DEFAULT_MAX_SIZE_POLICY;
        config.getCacheConfig(TEST_CACHE_NAME)
              .setInMemoryFormat(getInMemoryFormat())
              .getEvictionConfig()
              .setMaxSizePolicy(maxSizePolicy)
              .setSize(Integer.MAX_VALUE);

        return config;
    }

    @Test
    public void test() {
        ICache<Integer, Integer> cache = bounceMemberRule.getSteadyMember()
                                                         .getCacheManager()
                                                         .getCache(TEST_CACHE_NAME);
        populateCache(cache);

        Runnable[] testTasks = new Runnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; ) {
            HecatonInstance driver = bounceMemberRule.getNextTestDriver();
            testTasks[i++] = new IterationRunnable(driver);
            testTasks[i++] = new MutationRunnable(driver, i / 2);
        }
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(3));
    }

    private void populateCache(ICache<Integer, Integer> cache) {
        for (int i = 0; i < STABLE_ENTRY_COUNT; i++) {
            cache.put(i, i);
        }
    }

    public class IterationRunnable implements Runnable {

        private final HecatonInstance hecatonInstance;
        private ICache<Integer, Integer> cache;

        public IterationRunnable(HecatonInstance hecatonInstance) {
            this.hecatonInstance = hecatonInstance;
        }

        @Override
        public void run() {
            if (cache == null) {
                cache = hecatonInstance.getCacheManager().getCache(TEST_CACHE_NAME);
            }
            HashSet<Integer> all = getAll();
            for (int i = 0; i < STABLE_ENTRY_COUNT; i++) {
                assertTrue("Missing stable entry " + i + " - " + cache.get(i), all.contains(i));
            }

            logger.info("Successfully finished iteration " + successfullIterations.incrementAndGet());
        }

        private HashSet<Integer> getAll() {
            HashSet<Integer> keys = new HashSet<>();
            int partitionCount = hecatonInstance.getPartitionService().getPartitions().size();
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                Iterator<Cache.Entry<Integer, Integer>> iterator = createIterator(cache, FETCH_SIZE, partitionId, false);
                while (iterator.hasNext()) {
                    Entry<Integer, Integer> e = iterator.next();
                    assertTrue("Got the same key twice", keys.add(e.getKey()));
                }
            }
            return keys;
        }

    }

    private Iterator<Cache.Entry<Integer, Integer>> createIterator(
            ICache<Integer, Integer> cache, int fetchSize, int partitionId, boolean prefetchValues) {
        return isClientDriver()
                ? ((ClientCacheProxy<Integer, Integer>) cache).iterator(fetchSize, partitionId, prefetchValues)
                : ((CacheProxy<Integer, Integer>) cache).iterator(fetchSize, partitionId, prefetchValues);
    }

    protected boolean isClientDriver() {
        return false;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return CacheSimpleConfig.DEFAULT_IN_MEMORY_FORMAT;
    }

    public static class MutationRunnable implements Runnable {
        private final HecatonInstance hecatonInstance;
        private final int startIndex;
        private final int endIndex;
        private ICache<Integer, Integer> cache;

        public MutationRunnable(HecatonInstance hecatonInstance, int runnableIndex) {
            this.hecatonInstance = hecatonInstance;
            this.startIndex = runnableIndex * MUTATION_ENTRY_FACTOR * STABLE_ENTRY_COUNT;
            this.endIndex = startIndex + MUTATION_ENTRY_FACTOR * STABLE_ENTRY_COUNT;
        }

        @Override
        public void run() {
            if (cache == null) {
                cache = hecatonInstance.getCacheManager().getCache(TEST_CACHE_NAME);
            }

            for (int i = startIndex; i < endIndex; i++) {
                cache.put(i, i);
            }

            for (int i = startIndex; i < endIndex; i++) {
                cache.remove(i, i);
            }
        }
    }
}
