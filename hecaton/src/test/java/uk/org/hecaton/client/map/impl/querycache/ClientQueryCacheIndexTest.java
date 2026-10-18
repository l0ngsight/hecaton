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

package uk.org.hecaton.client.map.impl.querycache;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.querycache.utils.Employee;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.AssertTask;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheIndexTest extends HecatonTestSupport {

    private static final Predicate<Integer, Employee> TRUE_PREDICATE = Predicates.alwaysTrue();

    private TestHecatonFactory factory;

    @Before
    public void setUp() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        factory = new TestHecatonFactory();
        factory.newHecatonInstance(config);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void test_keySet_withPredicate() {
        String mapName = randomString();
        String cacheName = randomString();

        HecatonInstance client = factory.newHecatonClient();
        IMap<Integer, Employee> map = client.getMap(mapName);

        // populate map before construction of query cache.
        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addIndex(IndexType.SORTED, "id");

        // populate map after construction of query cache
        for (int i = putCount; i < 2 * putCount; i++) {
            map.put(i, new Employee(i));
        }

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * putCount - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("id >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void test_keySet_withPredicate_whenValuesAreNotCached() {
        String mapName = randomString();
        String cacheName = randomString();

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig.setDelaySeconds(1);
        queryCacheConfig.setBatchSize(3);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);

        HecatonInstance client = factory.newHecatonClient();
        IMap<Integer, Employee> map = client.getMap(mapName);

        // populate map before construction of query cache
        int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        cache.addIndex(IndexType.SORTED, "__key");

        // populate map after construction of query cache
        for (int i = putCount; i < 2 * putCount; i++) {
            map.put(i, new Employee(i));
        }

        // just choose arbitrary numbers for querying in order to prove whether #keySet with predicate is correctly working
        int equalsOrBiggerThan = 27;
        int expectedSize = 2 * putCount - equalsOrBiggerThan;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("__key >= " + equalsOrBiggerThan), cache);
    }

    @Test
    public void test_keySet_withPredicate_afterRemovals() {
        String mapName = randomString();
        String cacheName = randomString();

        HecatonInstance client = factory.newHecatonClient();
        IMap<Integer, Employee> map = client.getMap(mapName);

        int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addIndex(IndexType.SORTED, "id");

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #keySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertKeySetSizeEventually(expectedSize, Predicates.sql("id < " + smallerThan), cache);
    }

    @Test
    public void test_entrySet_withPredicate_whenValuesNotCached() {
        String mapName = randomString();
        String cacheName = randomString();

        HecatonInstance client = factory.newHecatonClient();
        IMap<Integer, Employee> map = client.getMap(mapName);

        int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        cache.addIndex(IndexType.SORTED, "id");

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 0;
        assertEntrySetSizeEventually(expectedSize, Predicates.sql("id < " + smallerThan), cache);
    }

    @Test
    public void test_entrySet_onIndexedKeys_whenValuesNotCached() {
        String mapName = randomString();
        String cacheName = randomString();

        HecatonInstance client = factory.newHecatonClient();
        IMap<Integer, Employee> map = client.getMap(mapName);

        int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);
        // here add index to key. (key --> integer; value --> Employee)
        cache.addIndex(IndexType.SORTED, "__key");

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertEntrySetSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
    }

    @Test
    public void test_values_withoutIndex_whenValuesNotCached() {
        String mapName = randomString();
        String cacheName = randomString();

        HecatonInstance client = factory.newHecatonClient();
        IMap<Integer, Employee> map = client.getMap(mapName);

        int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, false);

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 0;
        assertValuesSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
    }

    @Test
    public void test_values_withoutIndex_whenValuesCached() {
        String mapName = randomString();
        String cacheName = randomString();

        HecatonInstance client = factory.newHecatonClient();
        IMap<Integer, Employee> map = client.getMap(mapName);

        int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);

        for (int i = 17; i < putCount; i++) {
            map.remove(i);
        }

        // just choose arbitrary numbers to prove whether #entrySet with predicate is working
        int smallerThan = 17;
        int expectedSize = 17;
        assertValuesSizeEventually(expectedSize, Predicates.sql("__key < " + smallerThan), cache);
    }

    private void assertKeySetSizeEventually(final int expectedSize, final Predicate predicate,
                                            final QueryCache<Integer, Employee> cache) {
        AssertTask task = () -> {
            int size = cache.size();
            Set<Integer> keySet = cache.keySet(predicate);
            assertEquals("cache size = " + size, expectedSize, keySet.size());
        };

        assertTrueEventually(task, 15);
    }

    private void assertEntrySetSizeEventually(final int expectedSize, final Predicate predicate,
                                              final QueryCache<Integer, Employee> cache) {
        AssertTask task = () -> {
            int size = cache.size();
            Set<Map.Entry<Integer, Employee>> entries = cache.entrySet(predicate);
            assertEquals("cache size = " + size, expectedSize, entries.size());
        };

        assertTrueEventually(task, 15);
    }

    private void assertValuesSizeEventually(final int expectedSize, final Predicate predicate,
                                            final QueryCache<Integer, Employee> cache) {
        AssertTask task = () -> {
            int size = cache.size();
            Collection<Employee> values = cache.values(predicate);
            assertEquals("cache size = " + size, expectedSize, values.size());
        };

        assertTrueEventually(task, 15);
    }
}
