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

package uk.org.hecaton.test.starter.constructor.test;

import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.map.impl.DataAwareEntryEvent;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.starter.constructor.DataAwareEntryEventConstructor;
import uk.org.hecaton.internal.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataAwareEntryEventConstructorTest {

    @Test
    public void testConstructor() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        String key = "key";
        String newValue = "newValue";
        String oldValue = "oldValue";
        String mergingValue = "mergingValue";

        Member from = mock(Member.class);
        int eventType = EntryEventType.MERGED.getType();
        String source = UuidUtil.newUnsecureUuidString();
        Data dataKey = serializationService.toData(key);
        Data dataNewValue = serializationService.toData(newValue);
        Data dataOldValue = serializationService.toData(oldValue);
        Data dataMergingValue = serializationService.toData(mergingValue);

        DataAwareEntryEvent dataAwareEntryEvent = new DataAwareEntryEvent(from, eventType, source, dataKey, dataNewValue,
                dataOldValue, dataMergingValue, serializationService);

        DataAwareEntryEventConstructor constructor = new DataAwareEntryEventConstructor(DataAwareEntryEvent.class);
        DataAwareEntryEvent clonedDataAwareEntryEvent = (DataAwareEntryEvent) constructor.createNew(dataAwareEntryEvent);

        assertEquals(dataAwareEntryEvent.getName(), clonedDataAwareEntryEvent.getName());
        assertEquals(dataAwareEntryEvent.getMember(), clonedDataAwareEntryEvent.getMember());
        assertEquals(dataAwareEntryEvent.getEventType(), clonedDataAwareEntryEvent.getEventType());
        assertEquals(dataAwareEntryEvent.getSource(), clonedDataAwareEntryEvent.getSource());
        assertEquals(dataAwareEntryEvent.getKey(), clonedDataAwareEntryEvent.getKey());
        assertEquals(dataAwareEntryEvent.getKeyData(), clonedDataAwareEntryEvent.getKeyData());
        assertEquals(dataAwareEntryEvent.getValue(), clonedDataAwareEntryEvent.getValue());
        assertEquals(dataAwareEntryEvent.getNewValueData(), clonedDataAwareEntryEvent.getNewValueData());
        assertEquals(dataAwareEntryEvent.getOldValue(), clonedDataAwareEntryEvent.getOldValue());
        assertEquals(dataAwareEntryEvent.getOldValueData(), clonedDataAwareEntryEvent.getOldValueData());
        assertEquals(dataAwareEntryEvent.getMergingValue(), clonedDataAwareEntryEvent.getMergingValue());
        assertEquals(dataAwareEntryEvent.getMergingValueData(), clonedDataAwareEntryEvent.getMergingValueData());
    }
}
