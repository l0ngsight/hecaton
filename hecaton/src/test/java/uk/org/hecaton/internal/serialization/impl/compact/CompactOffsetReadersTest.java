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

package uk.org.hecaton.internal.serialization.impl.compact;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;

import static uk.org.hecaton.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactOffsetReadersTest {

    private static String[] createStringArray(int itemCount) {
        String[] arr = new String[itemCount + 1];
        for (int i = 1; i <= itemCount; i++) {
            // create a string made up of n copies of string s
            arr[i - 1] = String.join("", Collections.nCopies(i * 100, "x"));
        }
        arr[itemCount] = null;
        return arr;
    }

    @Parameterized.Parameter
    public int itemCount;

    @Parameterized.Parameters(name = "itemCount:{0}")
    public static Object[] parameters() {
        return new Object[]{1, 20, 42};
    }

    @Test
    public void testObjectWithDifferentOffsetReaders() {
        SerializationService serializationService = createSerializationService();
        String[] strArray = createStringArray(itemCount);
        GenericRecord expected = GenericRecordBuilder.compact("offsetReaderTestDTO")
                .setArrayOfString("arrayOfStr", strArray)
                .setInt32("i", 32)
                .setString("str", "hey")
                .build();

        Data data = serializationService.toData(expected);
        GenericRecord actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }
}
