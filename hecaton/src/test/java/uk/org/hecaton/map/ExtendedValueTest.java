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

import uk.org.hecaton.map.EntryLoader.MetadataAwareValue;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.map.EntryLoader.MetadataAwareValue.NO_TIME_SET;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class ExtendedValueTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNullValueIsNotAccepted() {
        new MetadataAwareValue<>(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullValueIsNotAccepted_withExpiration() {
        new MetadataAwareValue<>(null, -1);
    }

    @Test
    public void testWithNoDate() {
        MetadataAwareValue<String> entry = new MetadataAwareValue<>("value");
        assertEquals(NO_TIME_SET, entry.getExpirationTime());
    }
}
