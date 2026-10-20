/*
 * Copyright (c) 2026, The Hecaton Authors.
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

import java.lang.reflect.Method;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.TestObject;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Regenerates the serialization test fixtures whose bytes embed class names, so they match
 * the renamed package (docs/changes-from-hazelcast.md):
 * <ul>
 *   <li>{@code 1.serialization.compatibility.binary}, via upstream's
 *       {@code BinaryCompatibilityFileGenerator};</li>
 *   <li>{@code testHz3Object} and {@code testHz3ObjectLittleEndian}, the compatibility-format
 *       encoding of the object used by {@code CompatibilitySerializationServiceTest}.</li>
 * </ul>
 * Run by tools/fixtures/regenerate-serialization-fixtures.sh with the hecaton test classpath;
 * the working directory must be hecaton/src/test/resources.
 */
public class RegenerateSerializationFixtures {

    public static void main(String[] args) throws Exception {
        Class<?> generator = Class.forName(
                "uk.org.hecaton.nio.serialization.compatibility.BinaryCompatibilityFileGenerator");
        Method main = generator.getDeclaredMethod("main", String[].class);
        main.setAccessible(true);
        main.invoke(null, (Object) new String[0]);
        System.out.println("wrote 1.serialization.compatibility.binary");

        writeHz3Object(ByteOrder.BIG_ENDIAN, "testHz3Object");
        writeHz3Object(ByteOrder.LITTLE_ENDIAN, "testHz3ObjectLittleEndian");
    }

    private static void writeHz3Object(ByteOrder order, String file) throws Exception {
        InternalSerializationService service = (InternalSerializationService) new DefaultSerializationServiceBuilder()
                .setVersion(InternalSerializationService.VERSION_1)
                .setNotActiveExceptionSupplier(HecatonInstanceNotActiveException::new)
                .isCompatibility(true)
                .setByteOrder(order)
                .build();
        Map<Integer, String> map = new HashMap<>();
        map.put(42, "hello world");
        // Same construction as the test (Guava collections), so serialized capacities match.
        TestObject object = new TestObject(true, (byte) 42, 'd', (short) 42, 42, 42, 4.2f, 4.2, "hello world",
                newArrayList("hello world"), newHashSet("hello world"), map);
        Files.write(Path.of(file), service.toData(object).toByteArray());
        System.out.println("wrote " + file);
    }
}
