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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.operation.MergeOperation;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpiryMetadata;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.map.listener.EntryRemovedListener;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static uk.org.hecaton.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheIMapEventHandlingTest extends HecatonTestSupport {

    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    private HecatonInstance member;

    private String mapName;
    private IMap<Integer, Integer> map;
    private QueryCache<Integer, Integer> queryCache;

    @Before
    public void setUp() {
        member = createHecatonInstance();

        mapName = randomMapName();
        map = getMap(member, mapName);
        queryCache = map.getQueryCache("cqc", TRUE_PREDICATE, true);
    }

    @Test
    public void testEvent_MERGED() throws Exception {
        final int key = 1;
        final int existingValue = 1;
        final int mergingValue = 2;

        map.put(key, existingValue);

        executeMergeOperation(member, mapName, key, mergingValue);

        assertTrueEventually(() -> {
            Integer currentValue = queryCache.get(key);
            assertEquals(mergingValue, (Object) currentValue);
        });
    }

    private void executeMergeOperation(HecatonInstance member, String mapName, int key, int mergedValue) throws Exception {
        Node node = getNode(member);
        NodeEngineImpl nodeEngine = node.nodeEngine;
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        SerializationService serializationService = getSerializationService(member);

        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(mergedValue);
        SplitBrainMergeTypes.MapMergeTypes mergingEntry
                = createMergingEntry(serializationService, keyData, valueData,
                Mockito.mock(Record.class), ExpiryMetadata.NULL);

        Operation mergeOperation = new MergeOperation(mapName, singletonList(mergingEntry),
                new PassThroughMergePolicy<>(), false);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        Future<Object> future = operationService.invokeOnPartition(SERVICE_NAME, mergeOperation, partitionId);
        future.get();
    }

    @Test
    public void testEvent_EXPIRED() throws Exception {
        int key = 1;
        int value = 1;

        final CountDownLatch latch = new CountDownLatch(1);
        queryCache.addEntryListener((EntryAddedListener<Integer, Integer>) event -> latch.countDown(), true);

        map.put(key, value, 1, SECONDS);

        latch.await();
        sleepSeconds(1);

        // map#get creates EXPIRED event
        map.get(key);

        assertTrueEventually(() -> assertEquals(0, queryCache.size()));
    }

    @Test
    public void testListenerRegistration() {
        UUID addEntryListener = queryCache.addEntryListener((EntryAddedListener<Integer, Integer>) event -> {
        }, true);

        UUID removeEntryListener = queryCache.addEntryListener((EntryRemovedListener<Integer, Integer>) event -> {
        }, true);

        assertFalse(queryCache.removeEntryListener(UUID.randomUUID()));

        assertTrue(queryCache.removeEntryListener(removeEntryListener));
        assertFalse(queryCache.removeEntryListener(removeEntryListener));

        assertTrue(queryCache.removeEntryListener(addEntryListener));
        assertFalse(queryCache.removeEntryListener(addEntryListener));
    }
}
