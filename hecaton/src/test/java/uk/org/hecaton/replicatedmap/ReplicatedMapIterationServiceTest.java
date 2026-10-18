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

package uk.org.hecaton.replicatedmap;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.iteration.IterationResult;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.iterator.ReplicatedMapIterationService;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static uk.org.hecaton.replicatedmap.impl.iterator.ReplicatedMapIterationService.ITERATOR_CLEANUP_TIMEOUT_MILLIS;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapIterationServiceTest extends HecatonTestSupport {

    public static final int ITEM_COUNT = 1000;
    private ReplicatedMapIterationService service;
    private SerializationService ss;
    private HecatonInstance instance;

    @Before
    public void setUp() {
        instance = createHecatonInstance(getConfig());
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        ss = nodeEngine.getSerializationService();
        service = new ReplicatedMapIterationService(nodeEngine.getService(ReplicatedMapService.SERVICE_NAME), ss, nodeEngine);
    }

    @Override
    protected Config getConfig() {
        Config c = super.getConfig();
        c.getProperties().setProperty(ITERATOR_CLEANUP_TIMEOUT_MILLIS.getName(), "1000");
        return c;
    }

    @Test
    public void emptyReplicatedMap_createIterator() {
        String name = randomName();
        instance.getReplicatedMap(name);
        service.createIterator(name, 0, UuidUtil.newUnsecureUUID());
    }

    @Test
    public void nonExistingReplicatedMap_createIterator_throws() {
        String name = randomName();
        assertThatThrownBy(() -> service.createIterator(name, 0, UuidUtil.newUnsecureUUID())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("There is no ReplicatedRecordStore for " + name);
    }

    @Test
    public void removeStaleIterators_cleansStaleIterators() throws InterruptedException {
        String name = randomName();
        populateMap(instance.getReplicatedMap(name));
        UUID uuid = UuidUtil.newUnsecureUUID();
        service.createIterator(name, 0, uuid);
        assertThat(service.getIteratorManager().getKeySet()).containsExactlyInAnyOrder(uuid);
        Thread.sleep(1500);
        service.removeStaleIterators();
        assertThat(service.getIteratorManager().getKeySet()).isEmpty();
    }

    @Test
    public void iterate() {
        String name = randomName();
        populateMap(instance.getReplicatedMap(name));
        int partitionId = 12;
        UUID iteratorId = UuidUtil.newUnsecureUUID();
        service.createIterator(name, partitionId, iteratorId);
        int pageSize = 100;
        iterateAndAssert(iteratorId, pageSize, partitionId);
    }

    private void iterateAndAssert(UUID cursorId, int pageSize, int partitionId) {
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            if (instance.getPartitionService().getPartition(i).getPartitionId() == partitionId) {
                keys.add(i);
            }
        }
        IterationResult<ReplicatedMapEntryViewHolder> result = service.iterate(cursorId, pageSize);
        // first iteration does not forget a cursor id
        assertThat(result.getCursorIdToForget()).isNull();
        assertThat(result.getCursorId()).isNotNull();
        assertThat(result.getCursorId()).isNotEqualTo(cursorId);
        List<Integer> actualKeys = new ArrayList<>();
        assertThat(result.getPage()).hasSizeLessThanOrEqualTo(pageSize);
        for (ReplicatedMapEntryViewHolder entry : result.getPage()) {
            int key = ss.toObject(entry.getKey());
            int value = ss.toObject(entry.getValue());
            assertThat(key).isEqualTo(value);
            actualKeys.add(key);
        }
        assertThat(actualKeys).containsExactlyInAnyOrderElementsOf(keys);
    }

    private void populateMap(Map<Integer, Integer> map) {
        Map<Integer, Integer> hashMap = new HashMap<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            hashMap.put(i, i);
        }
        map.putAll(hashMap);
    }
}
