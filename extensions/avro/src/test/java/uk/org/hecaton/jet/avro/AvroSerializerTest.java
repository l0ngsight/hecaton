/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.avro;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import org.apache.avro.util.Utf8;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.util.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
public class AvroSerializerTest {

    static InternalSerializationService serializationService;

    @BeforeClass
    public static void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void testUtf8() {
        Utf8 expected = new Utf8(newUnsecureUuidString());
        Data data = serializationService.toData(expected);
        Utf8 actual = serializationService.toObject(data);
        assertEquals(expected, actual);
    }

}
