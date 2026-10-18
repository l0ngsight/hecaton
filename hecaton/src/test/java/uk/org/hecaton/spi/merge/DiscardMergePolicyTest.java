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

package uk.org.hecaton.spi.merge;

import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiscardMergePolicyTest {

    private static final SerializationService SERIALIZATION_SERVICE = new DefaultSerializationServiceBuilder().build();
    private static final Data EXISTING = SERIALIZATION_SERVICE.toData("EXISTING");
    private static final Data MERGING = SERIALIZATION_SERVICE.toData("MERGING");

    private SplitBrainMergePolicy<String, MapMergeTypes<Object, String>, Object> mergePolicy;

    @Before
    public void setup() {
        mergePolicy = new DiscardMergePolicy<>();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_existingValueAbsent() {
        MapMergeTypes existing = null;
        MapMergeTypes merging = mergingValueWithGivenValue(MERGING);

        assertNull(mergePolicy.merge(merging, existing));
    }

    @Test
    public void merge_existingValuePresent() {
        MapMergeTypes existing = mergingValueWithGivenValue(EXISTING);
        MapMergeTypes merging = mergingValueWithGivenValue(MERGING);

        assertEquals(EXISTING, mergePolicy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_mergingNull() {
        MapMergeTypes existing = mergingValueWithGivenValue(EXISTING);
        MapMergeTypes merging = null;

        assertEquals(EXISTING, mergePolicy.merge(merging, existing));
    }

    @Test
    public void merge_bothValuesNull() {
        MapMergeTypes existing = mergingValueWithGivenValue(null);
        MapMergeTypes merging = mergingValueWithGivenValue(null);

        assertNull(mergePolicy.merge(merging, existing));
    }

    private MapMergeTypes mergingValueWithGivenValue(Data value) {
        MapMergeTypes mergingValue = mock(MapMergeTypes.class);
        try {
            when(mergingValue.getRawValue()).thenReturn(value);
            return mergingValue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
