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

import uk.org.hecaton.core.EntryView;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.SimpleEntryView;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
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
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapMergePolicySerializationTest extends HecatonTestSupport {

    @Test
    public void testIssue2665() {
        String name = randomString();
        String serviceName = "hz:impl:mapService";

        HecatonInstance instance = createHecatonInstance(getConfig());
        IMap<String, MyObject> map = instance.getMap(name);
        MyObject myObjectExisting = new MyObject();
        map.put("key", myObjectExisting);

        NodeEngineImpl nodeEngine = getNode(instance).getNodeEngine();
        MapService mapService = nodeEngine.getService(serviceName);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionId = nodeEngine.getPartitionService().getPartitionId("key");

        MyObject myObject = new MyObject();
        Data dataKey = mapServiceContext.toData("key");
        Data dataValue = mapServiceContext.toData(myObject);

        RecordStore recordStore = mapServiceContext.getRecordStore(partitionId, name);
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName(), null);
        EntryView<Data, Data> mergingEntryView = new SimpleEntryView<>(dataKey, dataValue);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), mergingEntryView), mergePolicy, CallerProvenance.NOT_WAN);

        int deSerializedCount = MyObject.deserializedCount;
        assertEquals(0, deSerializedCount);
    }

    private static class MyObject implements DataSerializable {

        static int serializedCount = 0;
        static int deserializedCount = 0;

        MyObject() {
        }

        @Override
        public void writeData(ObjectDataOutput out) {
            serializedCount += 1;
        }

        @Override
        public void readData(ObjectDataInput in) {
            deserializedCount += 1;
        }
    }
}
