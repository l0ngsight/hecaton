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

package uk.org.hecaton.map.impl.query;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.util.collection.PartitionIdSet;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static uk.org.hecaton.query.Predicates.partitionPredicate;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapKeySetTest extends HecatonTestSupport {

    private HecatonInstance instance;
    private IMap<String, String> map;
    private SerializationService serializationService;

    @Before
    public void setup() {
        Config config = regularInstanceConfig();
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        config.getMapConfig("indexed").addIndexConfig(new IndexConfig(IndexType.SORTED, "this"));
        instance = createHecatonInstance(config);
        map = instance.getMap(randomName());
        serializationService = getSerializationService(instance);
    }

    @Test(expected = NullPointerException.class)
    public void whenPredicateNull() {
        map.entrySet(null);
    }

    @Test
    public void whenMapEmpty() {
        Set<String> result = map.keySet(Predicates.alwaysTrue());

        assertTrue(result.isEmpty());
    }

    @Test
    public void whenSelecting_withoutPredicate() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Set<String> result = map.keySet();

        assertEquals(Set.of("1", "2", "3"), result);
    }

    @Test
    public void whenSelectingAllEntries() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Set<String> result = map.keySet(Predicates.alwaysTrue());

        assertEquals(Set.of("1", "2", "3"), result);
    }

    @Test
    public void whenSelectingSomeEntries() {
        map.put("1", "good1");
        map.put("2", "bad");
        map.put("3", "good2");

        Set<String> result = map.keySet(new GoodPredicate());

        assertEquals(Set.of("1", "3"), result);
    }

    @Test
    public void whenSelectingPartitionSubset() {
        PartitionIdSet partitionSubset = new PartitionIdSet(4, asList(1, 3));
        Set<String> matchingKeys = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            String key = generateKeyForPartition(instance, i);
            map.put(key, key);
            if (partitionSubset.contains(i)) {
                matchingKeys.add(key);
            }
        }

        Set<String> result = ((MapProxyImpl<String, String>) map).keySet(Predicates.alwaysTrue(), partitionSubset);
        assertEquals(matchingKeys, result);
    }

    @Test
    public void whenSelectingPartitionSubset_withIndex() {
        PartitionIdSet partitionSubset = new PartitionIdSet(4, asList(1, 3));
        Set<String> matchingKeys = new HashSet<>();
        map = instance.getMap("indexed");
        for (int i = 0; i < 5; i++) {
            String key = generateKeyForPartition(instance, i);
            map.put(key, key);
            if (partitionSubset.contains(i)) {
                matchingKeys.add(key);
            }
        }

        // "" is sorted before any non-null string - internally all items from all partitions are added
        // to the result (because the index is global), but there's a code that eliminates partitions not
        // in the subset - this test aims to test that code
        Predicate<String, String> predicate = Predicates.greaterThan("this", "");
        Set<String> result =
                ((MapProxyImpl<String, String>) map).keySet(predicate, partitionSubset);
        assertEquals(2, result.size());
        assertEquals(matchingKeys, result);
    }

    @Test
    public void when_selectingPartitionSubset_and_partitionPredicate() {
        PartitionIdSet partitionSubset = new PartitionIdSet(4, asList(1, 3));
        Set<String> matchingKeys = new HashSet<>();
        String key1 = null;
        for (int i = 0; i < 5; i++) {
            String key = generateKeyForPartition(instance, i);
            if (i == 1) {
                key1 = key;
            }
            map.put(key, key);
            if (partitionSubset.contains(i)) {
                matchingKeys.add(key);
            }
        }

        Set<String> result = ((MapProxyImpl<String, String>) map)
                .keySet(partitionPredicate(key1, Predicates.alwaysTrue()), partitionSubset);
        assertEquals(Collections.singleton(key1), result);
    }

    @Test
    public void testResultType() {
        map.put("1", "a");
        Set<String> entries = map.keySet(Predicates.alwaysTrue());

        QueryResultCollection collection = assertInstanceOf(QueryResultCollection.class, entries);
        QueryResultRow row = (QueryResultRow) collection.getRows().iterator().next();
        assertEquals(serializationService.toData("1"), row.getKey());
        assertNull(row.getValue());
    }

    static class GoodPredicate implements Predicate<String, String> {
        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
            return mapEntry.getValue().startsWith("good");
        }
    }
}
