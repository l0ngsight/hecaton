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

package uk.org.hecaton.client.map;

import uk.org.hecaton.aggregation.Aggregators;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.projection.Projection;
import uk.org.hecaton.query.PagingPredicate;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.properties.ClusterProperty;
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

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPartitionPredicateTest extends HecatonTestSupport {

    private static final int PARTITIONS = 10;
    private static final int ITEMS_PER_PARTITION = 20;

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance server;
    private HecatonInstance client;
    private IMap<String, Integer> map;

    private String partitionKey;
    private int partitionId;

    private Predicate<String, Integer> predicate;

    @Before
    public void setUp() {
        Config config = getConfig().setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITIONS));

        server = hecatonFactory.newHecatonInstance(config);
        HecatonInstance remote = hecatonFactory.newHecatonInstance(config);
        warmUpPartitions(server, remote);

        client = hecatonFactory.newHecatonClient();

        String mapName = randomString();
        map = client.getMap(mapName);

        for (int p = 0; p < PARTITIONS; p++) {
            for (int k = 0; k < ITEMS_PER_PARTITION; k++) {
                map.put(generateKeyForPartition(server, p), p);
            }
        }
        partitionKey = randomString();
        partitionId = server.getPartitionService().getPartition(partitionKey).getPartitionId();
        predicate = Predicates.partitionPredicate(partitionKey, Predicates.alwaysTrue());
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void values_withPagingPredicate() {
        PagingPredicate<String, Integer> pagingPredicate = Predicates.pagingPredicate(Predicates.alwaysTrue(), 1);
        predicate = Predicates.partitionPredicate(randomString(), pagingPredicate);

        for (int i = 0; i < ITEMS_PER_PARTITION; i++) {
            int size = map.values(predicate).size();
            assertEquals(1, size);
            pagingPredicate.nextPage();
        }
        int size = map.values(predicate).size();
        assertEquals(0, size);
    }

    @Test
    public void keys_withPagingPredicate() {
        PagingPredicate<String, Integer> pagingPredicate = Predicates.pagingPredicate(Predicates.alwaysTrue(), 1);
        predicate = Predicates.partitionPredicate(randomString(), pagingPredicate);

        for (int i = 0; i < ITEMS_PER_PARTITION; i++) {
            int size = map.keySet(predicate).size();
            assertEquals(1, size);
            pagingPredicate.nextPage();
        }
        int size = map.keySet(predicate).size();
        assertEquals(0, size);
    }

    @Test
    public void entries_withPagingPredicate() {
        PagingPredicate<String, Integer> pagingPredicate = Predicates.pagingPredicate(Predicates.alwaysTrue(), 1);
        predicate = Predicates.partitionPredicate(randomString(), pagingPredicate);

        for (int i = 0; i < ITEMS_PER_PARTITION; i++) {
            int size = map.entrySet(predicate).size();
            assertEquals(1, size);
            pagingPredicate.nextPage();
        }
        int size = map.entrySet(predicate).size();
        assertEquals(0, size);
    }

    @Test
    public void values() {
        Collection<Integer> values = map.values(predicate);

        assertEquals(ITEMS_PER_PARTITION, values.size());
        for (Integer value : values) {
            assertEquals(partitionId, value.intValue());
        }
    }

    @Test
    public void keySet() {
        Collection<String> keys = map.keySet(predicate);

        assertEquals(ITEMS_PER_PARTITION, keys.size());
        for (String key : keys) {
            assertEquals(partitionId, server.getPartitionService().getPartition(key).getPartitionId());
        }
    }

    @Test
    public void entries() {
        Collection<Map.Entry<String, Integer>> entries = map.entrySet(predicate);

        assertEquals(ITEMS_PER_PARTITION, entries.size());
        for (Map.Entry<String, Integer> entry : entries) {
            assertEquals(partitionId, server.getPartitionService().getPartition(entry.getKey()).getPartitionId());
            assertEquals(partitionId, entry.getValue().intValue());
        }
    }

    @Test
    public void aggregate() {
        int partitionId = 2;
        String keyForPartition = generateKeyForPartition(server, partitionId);
        Predicate partitionPredicate = Predicates.partitionPredicate(keyForPartition, Predicates.alwaysTrue());
        Long aggregate = map.aggregate(Aggregators.integerSum(), partitionPredicate);
        Long sum = (long) (partitionId * ITEMS_PER_PARTITION);
        assertEquals(sum, aggregate);
    }

    @Test
    public void executeOnEntries() {
        int partitionId = 2;
        String keyForPartition = generateKeyForPartition(server, partitionId);
        Predicate partitionPredicate = Predicates.partitionPredicate(keyForPartition, Predicates.alwaysTrue());
        Map<String, Object> entries = map.executeOnEntries(new MyProcessor(), partitionPredicate);
        assertEquals(ITEMS_PER_PARTITION, entries.size());
    }

    @Test
    public void removeAll() {
        int sizeBefore = map.size();
        int partitionSizeBefore = map.keySet(predicate).size();

        map.removeAll(predicate);
        assertEquals(sizeBefore - partitionSizeBefore, map.size());
        assertEquals(0, map.keySet(predicate).size());

        for (int i = 0; i < ITEMS_PER_PARTITION; ++i) {
            String key;
            do {
                key = generateKeyForPartition(server, partitionId);
            } while (map.containsKey(key));

            map.put(key, i);
        }
        sizeBefore = map.size();
        partitionSizeBefore = map.keySet(predicate).size();
        assertEquals(ITEMS_PER_PARTITION, partitionSizeBefore);
        map.removeAll(Predicates.partitionPredicate(partitionKey, Predicates.equal("this", ITEMS_PER_PARTITION - 1)));
        assertEquals(sizeBefore - 1, map.size());
        assertEquals(partitionSizeBefore - 1, map.keySet(predicate).size());
    }

    static class MyProcessor implements EntryProcessor<String, Integer, Map.Entry<String, Integer>> {

        MyProcessor() {
        }

        @Override
        public Map.Entry<String, Integer> process(Map.Entry<String, Integer> entry) {
            Integer in = entry.getValue();
            entry.setValue(in * 10);
            return entry;
        }
    }

    @Test
    public void project() {
        Predicate partitionPredicate = Predicates.partitionPredicate(1, Predicates.alwaysTrue());
        Collection<Integer> collection = map.project(new PrimitiveValueIncrementingProjection(), partitionPredicate);
        assertEquals(ITEMS_PER_PARTITION, collection.size());
    }

    public static class PrimitiveValueIncrementingProjection implements Projection<Map.Entry<String, Integer>, Integer> {
        @Override
        public Integer transform(Map.Entry<String, Integer> input) {
            return input.getValue() + 1;
        }
    }

}
