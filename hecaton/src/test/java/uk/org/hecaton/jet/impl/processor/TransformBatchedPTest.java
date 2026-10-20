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

import uk.org.hecaton.jet.Traversers;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.jet.Traversers.traverseIterable;
import static java.util.Arrays.asList;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransformBatchedPTest extends JetTestSupport {

    @Test
    public void test_process() {
        TestSupport
                .verifyProcessor(() -> new TransformBatchedP<>(
                        (Iterable<Integer> items) -> traverseIterable(items)
                                .flatMap(i -> Traversers.traverseItems(i * 2, i * 2 + 1))
                ))
                .input(asList(1, 2, 3, 4, 5))
                .expectOutput(asList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11));
    }
}
