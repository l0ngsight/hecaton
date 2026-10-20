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

package uk.org.hecaton.jet.impl.serialization;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.jet.accumulator.LinTrendAccumulator;
import uk.org.hecaton.jet.accumulator.LongAccumulator;
import uk.org.hecaton.jet.accumulator.LongDoubleAccumulator;
import uk.org.hecaton.jet.accumulator.LongLongAccumulator;
import uk.org.hecaton.jet.accumulator.MutableReference;
import uk.org.hecaton.jet.accumulator.PickAnyAccumulator;
import uk.org.hecaton.jet.core.Watermark;
import uk.org.hecaton.jet.datamodel.KeyedWindowResult;
import uk.org.hecaton.jet.datamodel.TimestampedItem;
import uk.org.hecaton.jet.datamodel.WindowResult;
import uk.org.hecaton.jet.impl.execution.BroadcastEntry;
import uk.org.hecaton.jet.impl.execution.SnapshotBarrier;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigInteger;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;

import static uk.org.hecaton.jet.core.BroadcastKey.broadcastKey;
import static uk.org.hecaton.jet.datamodel.ItemsByTag.itemsByTag;
import static uk.org.hecaton.jet.datamodel.Tag.tag;
import static uk.org.hecaton.jet.datamodel.Tuple2.tuple2;
import static uk.org.hecaton.jet.datamodel.Tuple3.tuple3;
import static uk.org.hecaton.jet.datamodel.Tuple4.tuple4;
import static uk.org.hecaton.jet.datamodel.Tuple5.tuple5;
import static uk.org.hecaton.jet.impl.JetEvent.jetEvent;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SerializerHooksTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList(
                new Object[]{new String[]{"a", "b", "c"}},
                new SimpleImmutableEntry<>("key", "value"),
                jetEvent(System.currentTimeMillis(), "payload"),
                new LongAccumulator(42),
                new LongLongAccumulator(42, 45),
                new LongDoubleAccumulator(42, 0.42),
                new MutableReference<>("payload"),
                new LinTrendAccumulator(42, BigInteger.ONE, BigInteger.TEN,
                        BigInteger.valueOf(42), BigInteger.valueOf(43)),
                new PickAnyAccumulator<>("picked"),
                tuple2("a", "b"),
                tuple3("a", "b", "c"),
                tuple4("a", "b", "c", "d"),
                tuple5("a", "b", "c", "d", "e"),
                itemsByTag(tag(1), "value"),
                new Watermark(42),
                new SnapshotBarrier(43, true),
                new BroadcastEntry<>("key", "value"),
                broadcastKey("key2"),
                new WindowResult<>(433, 423, "result"),
                new KeyedWindowResult<>(433, 423, "key", "result"),
                new TimestampedItem<>(423, "result")
        );
    }

    @Test
    public void testSerializerHooks() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);

        assertNotSame("serialization/deserialization didn't take place", instance, deserialized);
        if (instance instanceof Object[] objectArray) {
            assertArrayEquals("objects are not equal after serialize/deserialize",
                    objectArray, (Object[]) deserialized);
        } else {
            assertEquals("objects are not equal after serialize/deserialize", instance, deserialized);
        }
    }
}
