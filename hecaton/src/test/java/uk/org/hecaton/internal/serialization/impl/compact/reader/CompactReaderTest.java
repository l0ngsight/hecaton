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

package uk.org.hecaton.internal.serialization.impl.compact.reader;

import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.nio.serialization.FieldKind;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.nio.serialization.compact.CompactReader;
import uk.org.hecaton.nio.serialization.compact.CompactSerializer;
import uk.org.hecaton.nio.serialization.compact.CompactWriter;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import static uk.org.hecaton.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactReaderTest {

    @Test
    public void testGetFieldKind() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .addSerializer(new FooSerializer());

        SerializationService service = createSerializationService(config);

        Foo foo = new Foo(1);
        Data data = service.toData(foo);
        Foo deserialized = service.toObject(data);
        assertEquals(1, deserialized.i);
    }

    @Test
    public void testGetFieldKind_whenFieldDoesNotExist() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .addSerializer(new FooSerializer());

        SerializationService service = createSerializationService(config);

        // Same name, different kind
        GenericRecord record = GenericRecordBuilder.compact("foo")
                .setString("i", "1")
                .build();
        Data data = service.toData(record);
        Foo deserialized = service.toObject(data);
        assertEquals(42, deserialized.i);

        // Different name, different kind
        record = GenericRecordBuilder.compact("foo")
                .setString("ii", "1")
                .build();
        data = service.toData(record);
        deserialized = service.toObject(data);
        assertEquals(42, deserialized.i);
    }

    private static final class Foo {
        private final int i;

        Foo(int i) {
            this.i = i;
        }
    }

    private static final class FooSerializer implements CompactSerializer<Foo> {
        @Nonnull
        @Override
        public Foo read(@Nonnull CompactReader reader) {
            if (reader.getFieldKind("i") == FieldKind.INT32) {
                return new Foo(reader.readInt32("i"));
            } else {
                return new Foo(42);
            }
        }

        @Override
        public void write(@Nonnull CompactWriter writer, @Nonnull Foo object) {
            writer.writeInt32("i", object.i);
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "foo";
        }

        @Nonnull
        @Override
        public Class<Foo> getCompactClass() {
            return Foo.class;
        }
    }
}
