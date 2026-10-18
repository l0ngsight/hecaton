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
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.querycache.utils.Employee;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;

import static uk.org.hecaton.map.impl.event.MapEventPublisherImpl.LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES;

public abstract class AbstractQueryCacheTestSupport extends HecatonTestSupport {

    protected String mapName = randomString();
    protected String cacheName = randomString();
    protected TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);

    InMemoryFormat getInMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    void populateMap(IMap<Integer, Employee> map, int count) {
        populateMap(map, 0, count);
    }

    void populateMap(IMap<Integer, Employee> map, int startIndex, int endIndex) {
        for (int i = startIndex; i < endIndex; i++) {
            map.put(i, new Employee(i));
        }
    }

    void removeEntriesFromMap(IMap<Integer, Employee> map, int startIndex, int endIndex) {
        for (int i = startIndex; i < endIndex; i++) {
            map.remove(i);
        }
    }

    <K, V> IMap<K, V> getIMap(Config config) {
        return factory.newInstances(config)[0].getMap(mapName);
    }

    <K, V> IMap<K, V> getIMapWithDefaultConfig(Predicate predicate) {
        String defaultValue = LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES.getDefaultValue();
        return getIMapWithDefaultConfig(predicate, defaultValue);
    }

    <K, V> IMap<K, V> getIMapWithDefaultConfig(Predicate predicate, String useNaturalFilteringStrategy) {
        Config config = new Config();
        config.setProperty("hecaton.map.entry.filtering.natural.event.types", useNaturalFilteringStrategy);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig.getPredicateConfig().setImplementation(predicate);
        queryCacheConfig.setInMemoryFormat(getInMemoryFormat());
        config.getMapConfig(mapName).addQueryCacheConfig(queryCacheConfig);

        return factory.newInstances(config)[0].getMap(mapName);
    }

    public static <K, V> IMap<K, V> getMap(HecatonInstance instance, String mapName) {
        return instance.getMap(mapName);
    }


}
