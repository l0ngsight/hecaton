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

package uk.org.hecaton.jet.impl.processor;

import uk.org.hecaton.function.Functions;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.aggregate.AggregateOperations;
import uk.org.hecaton.jet.core.TimestampKind;
import uk.org.hecaton.jet.core.processor.Processors;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map.Entry;

import static uk.org.hecaton.function.Functions.entryKey;
import static uk.org.hecaton.jet.Util.entry;
import static uk.org.hecaton.jet.aggregate.AggregateOperations.aggregateOperation2;
import static uk.org.hecaton.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Category(ParallelJVMTest.class)
@RunWith(HecatonParallelClassRunner.class)
public class SlidingWindowP_CoGroupTest {

    @Test
    @SuppressWarnings("unchecked")
    public void test() {
        SupplierEx supplier = Processors.aggregateToSlidingWindowP(
                asList(Functions.<String>entryKey(), entryKey()),
                asList(t -> 1L, t -> 1L),
                TimestampKind.FRAME,
                tumblingWinPolicy(1),
                0L,
                aggregateOperation2(
                        AggregateOperations.<Entry<String, String>>toList(),
                        AggregateOperations.<Entry<String, String>>toList()),
                (start, end, key, result, isEarly) -> result(end, key, result.f0(), result.f1()));
        Entry<String, String> entry1 = entry("k1", "a");
        Entry<String, String> entry2 = entry("k2", "b");
        Entry<String, String> entry3 = entry("k1", "c");
        Entry<String, String> entry4 = entry("k3", "d");
        Entry<String, String> entry5 = entry("k1", "e");
        TestSupport.verifyProcessor(supplier)
                   .inputs(asList(
                           asList(entry1, entry2),
                           asList(entry3, entry4, entry5)
                   ))
                   .expectOutput(asList(
                           result(1, "k1", singletonList(entry1), asList(entry3, entry5)),
                           result(1, "k2", singletonList(entry2), emptyList()),
                           result(1, "k3", emptyList(), singletonList(entry4))
                   ));
    }

    private static String result(
            long winEnd,
            String key,
            List<Entry<String, String>> result1,
            List<Entry<String, String>> result2
    ) {
        return String.format("(%03d, %s: %s, %s)", winEnd, key, result1, result2);
    }
}
