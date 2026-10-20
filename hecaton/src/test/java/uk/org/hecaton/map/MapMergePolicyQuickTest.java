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

package uk.org.hecaton.map;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.SimpleEntryView;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.merge.LatestUpdateMergePolicy;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.PutIfAbsentMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.impl.CallerProvenance;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapMergePolicyQuickTest extends HecatonTestSupport {

    @Test
    public void testLatestUpdateMapMergePolicy() {
        HecatonInstance instance = createHecatonInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");
        Data dataValue = mapServiceContext.toData("value1");
        Data dataValue2 = mapServiceContext.toData("value2");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        recordStore.beforeOperation();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(LatestUpdateMergePolicy.class.getName(), null);
        long now = Clock.currentTimeMillis();

        SimpleEntryView<Data, Data> initialEntry = new SimpleEntryView<>(dataKey, dataValue);
        initialEntry.setCreationTime(now);
        initialEntry.setLastUpdateTime(now);
        // need some latency to be sure that target members time is greater than now
        sleepMillis(100);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), initialEntry), mergePolicy, CallerProvenance.NOT_WAN);

        SimpleEntryView<Data, Data> mergingEntry = new SimpleEntryView<>(dataKey, dataValue2);
        now = Clock.currentTimeMillis();
        mergingEntry.setCreationTime(now);
        mergingEntry.setLastUpdateTime(now);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), mergingEntry), mergePolicy, CallerProvenance.NOT_WAN);

        assertEquals("value2", map.get("key"));
        recordStore.afterOperation();
    }

    @Test
    public void testPutIfAbsentMapMergePolicy() {
        HecatonInstance instance = createHecatonInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");
        Data dataValue = mapServiceContext.toData("value1");
        Data dataValue2 = mapServiceContext.toData("value2");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        recordStore.beforeOperation();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName(), null);

        SimpleEntryView<Data, Data> initialEntry = new SimpleEntryView<>(dataKey, dataValue);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), initialEntry), mergePolicy, CallerProvenance.NOT_WAN);

        SimpleEntryView<Data, Data> mergingEntry = new SimpleEntryView<>(dataKey, dataValue2);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), mergingEntry), mergePolicy, CallerProvenance.NOT_WAN);

        assertEquals("value1", map.get("key"));
        recordStore.afterOperation();
    }

    @Test
    public void testPassThroughMapMergePolicy() {
        HecatonInstance instance = createHecatonInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");
        Data dataValue = mapServiceContext.toData("value1");
        Data dataValue2 = mapServiceContext.toData("value2");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        recordStore.beforeOperation();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(PassThroughMergePolicy.class.getName(), null);

        SimpleEntryView<Data, Data> initialEntry = new SimpleEntryView<>(dataKey, dataValue);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), initialEntry), mergePolicy, CallerProvenance.NOT_WAN);

        SimpleEntryView<Data, Data> mergingEntry = new SimpleEntryView<>(dataKey, dataValue2);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), mergingEntry), mergePolicy, CallerProvenance.NOT_WAN);

        assertEquals("value2", map.get("key"));
        recordStore.afterOperation();
    }

    private MapServiceContext getMapServiceContext(HecatonInstance instance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }
}
