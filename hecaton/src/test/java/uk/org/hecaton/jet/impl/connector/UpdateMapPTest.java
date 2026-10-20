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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static uk.org.hecaton.jet.impl.connector.AsyncHecatonWriterP.MAX_PARALLEL_ASYNC_OPS_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UpdateMapPTest extends JetTestSupport {

    private static final int COUNT_PER_KEY = 16;

    @Parameterized.Parameter
    public int asyncLimit;

    @Parameterized.Parameter(1)
    public int keyRange;

    private HecatonInstance hz;
    private HecatonInstance client;
    private IMap<String, Integer> sinkMap;

    @Parameterized.Parameters(name = "asyncLimit: {0}, keyRange: {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[]{1, 4},
            new Object[]{1, 1024},
            new Object[]{MAX_PARALLEL_ASYNC_OPS_DEFAULT, 4},
            new Object[]{MAX_PARALLEL_ASYNC_OPS_DEFAULT, 1024}
        );
    }

    @Before
    public void setup() {
        hz = createHecatonInstance();
        client = createHecatonClient();
        sinkMap = hz.getMap("results");
    }

    @Test
    public void test_localMap() {
        runTest(updateMap(hz));
    }

    @Test
    public void test_localMap_with_EP() {
        runTest(updateMapWithEP(hz));
    }

    @Test
    public void test_remoteMap() {
        runTest(updateMap(client));

    }

    @Test
    public void test_remoteMap_with_EP() {
        runTest(updateMapWithEP(client));

    }

    private SupplierEx<Processor> updateMap(HecatonInstance instance) {
        return () -> new UpdateMapP<Integer, String, Integer>(
            instance,
            asyncLimit,
            sinkMap.getName(),
            Object::toString,
            (prev, next) -> {
                if (prev == null) {
                    return 1;
                }
                return prev + 1;
            });
    }

    private SupplierEx<Processor> updateMapWithEP(HecatonInstance instance) {
        return () -> new UpdateMapWithEntryProcessorP<Integer, String, Integer, Void>(
                    instance,
                    asyncLimit,
                    sinkMap.getName(),
                    Object::toString,
                    i -> new IncrementEntryProcessor());
    }

    private void runTest(SupplierEx<Processor> sup) {
        List<Integer> input = IntStream.range(0, keyRange * COUNT_PER_KEY)
                                       .map(i -> i % keyRange)
                                       .boxed()
                                       .collect(Collectors.toList());

        TestSupport
            .verifyProcessor(sup)
            .hecatonInstance(hz)
            .input(input)
            .disableSnapshots()
            .disableLogging()
            .disableProgressAssertion()
            .assertOutput(0, (mode, output) -> {
                for (int i = 0; i < keyRange; i++) {
                    assertEquals(Integer.valueOf(COUNT_PER_KEY), sinkMap.get(String.valueOf(i)));
                }
                sinkMap.clear();
            });
    }

    private static class IncrementEntryProcessor implements EntryProcessor<String, Integer, Void> {

        @Override
        public Void process(Entry<String, Integer> entry) {
            Integer val = entry.getValue();
            entry.setValue(val == null ? 1 : val + 1);
            return null;
        }
    }
}
