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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.core.processor.SourceProcessors;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.projection.Projection;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static uk.org.hecaton.jet.TestContextSupport.adaptSupplier;
import static uk.org.hecaton.jet.Util.entry;
import static uk.org.hecaton.jet.core.Edge.between;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadMapOrCachePTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void setUp() {
        initialize(1, null);
    }

    @Test
    public void test_whenEmpty() {
        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(randomMapName())))
                .hecatonInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(emptyList());
    }

    @Test
    public void test_whenNoPredicateAndNoProjection() {
        IMap<Integer, String> map = instance().getMap(randomMapName());
        List<Entry<Integer, String>> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            map.put(i, "value-" + i);
            expected.add(entry(i, "value-" + i));
        }

        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(map.getName())))
                .hecatonInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectOutput(expected);
    }

    @Test
    public void test_whenPredicateAndProjectionSet() {
        IMap<Integer, String> map = instance().getMap(randomMapName());
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            map.put(i, "value-" + i);
            if (i % 2 == 0) {
                expected.add("value-" + i);
            }
        }

        Predicate<Integer, String> predicate = entry -> entry.getKey() % 2 == 0;
        Projection<Entry<Integer, String>, String> projection = toProjection(Entry::getValue);
        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(map.getName(), predicate, projection)))
                .hecatonInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectOutput(expected);
    }

    @Test
    public void test_largeMap() {
        IMap<Integer, Integer> map = instance().getMap(randomMapName());
        Map<Integer, Integer> tmpMap = new HashMap<>();
        int numItems = 500_000;
        for (Integer i = 0; i < numItems; i++) {
            tmpMap.put(i, i);
            if (tmpMap.size() == 10_000) {
                map.putAll(tmpMap);
                tmpMap.clear();
            }
        }

        CheckItemsP.received = new BitSet(numItems);

        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", SourceProcessors.readMapP(map.getName()));
        Vertex dest = dag.newVertex("dest", CheckItemsP::new).localParallelism(1);
        dag.edge(between(src, dest));

        instance().getJet().newJob(dag).join();
        assertEquals(numItems, CheckItemsP.received.cardinality());
        assertEquals(numItems, CheckItemsP.received.length());

        map.destroy();
    }

    private static final class CheckItemsP extends AbstractProcessor {
        static BitSet received;
        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            @SuppressWarnings("unchecked")
            int value = ((Entry<Integer, Integer>) item).getValue();
            assertFalse(received.get(value));
            received.set(value);
            return true;
        }
    }

    @Test
    public void test_whenProjectedToObjectWithNoEquals() {
        // test for https://github.com/hazelcast/hazelcast-jet/issues/2448
        IMap<Integer, Object[]> map = instance().getMap(randomMapName());
        // two values are enough: TestSupport always uses outbox limited to 1 item to drive the processors crazy
        map.put(0, new Object[0]);
        map.put(1, new Object[0]);

        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readMapP(map.getName())))
                .hecatonInstance(instance())
                .disableSnapshots()
                .disableProgressAssertion()
                .outputChecker((expected, actual) -> 2 == actual.size())
                .expectOutput(emptyList());
    }

    private static <I, O> Projection<I, O> toProjection(FunctionEx<I, O> projectionFn) {
        return projectionFn::apply;
    }
}
