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

package uk.org.hecaton.map.impl.querycache;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapLoader;
import uk.org.hecaton.map.MapStoreAdapter;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheMapLoaderTest extends HecatonTestSupport {

    @Parameterized.Parameter(0)
    public MapLoader mapLoader;

    @Parameterized.Parameter(1)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "{0}, inMemoryFormat {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {new DefaultMapLoader(), BINARY},
                {new SlowMapLoader(1), BINARY},
                {new DefaultMapLoader(), OBJECT},
                {new SlowMapLoader(1), OBJECT}
        });
    }

    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    @Test
    public void testQueryCache_includesLoadedEntries_after_get() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = getConfig(mapName, cacheName);
        HecatonInstance node = createHecatonInstance(config);
        IMap<Integer, Integer> map = getMap(node, mapName);

        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);

        map.get(1);
        map.get(2);
        map.get(3);

        assertTrueEventually(() -> assertEquals(3, cache.size()));
    }

    @Test
    public void testQueryCache_includesLoadedEntries_after_getAll() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = getConfig(mapName, cacheName);
        HecatonInstance node = createHecatonInstance(config);
        IMap<Integer, Integer> map = getMap(node, mapName);

        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);

        map.getAll(Set.of(1, 2, 3));

        assertTrueEventually(() -> assertEquals(3, cache.size()));
    }

    private Config getConfig(String mapName, String cacheName) {
        Config config = getConfig();

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        mapConfig.getMapStoreConfig()
                .setEnabled(true)
                .setImplementation(mapLoader);

        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        mapConfig.addQueryCacheConfig(cacheConfig);
        return config;
    }

    static class DefaultMapLoader extends MapStoreAdapter<Integer, Integer> {

        private final ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<>();

        DefaultMapLoader() {
            map.put(1, 1);
            map.put(2, 2);
            map.put(3, 4);
        }

        @Override
        public Integer load(Integer key) {
            return map.get(key);
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            Map<Integer, Integer> results = new HashMap<>();
            for (Integer key : keys) {
                results.put(key, map.get(key));
            }
            return results;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return Collections.emptySet();
        }

        @Override
        public String toString() {
            return "TestMapLoader";
        }
    }

    /**
     * for every method call, first sleep then do method call.
     */
    static class SlowMapLoader extends DefaultMapLoader {

        private final int sleepSeconds;

        SlowMapLoader(int sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        @Override
        public Integer load(Integer key) {
            sleepSeconds(sleepSeconds);

            return super.load(key);
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            sleepSeconds(sleepSeconds);

            return super.loadAll(keys);
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            sleepSeconds(sleepSeconds);

            return super.loadAllKeys();
        }

        @Override
        public String toString() {
            return "SlowMapLoader";
        }
    }
}
