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

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.processor.SourceProcessors;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.IntStream;

import static uk.org.hecaton.jet.TestContextSupport.adaptSupplier;
import static java.util.stream.Collectors.toList;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadIListPTest extends JetTestSupport {

    private HecatonInstance instance;

    @Before
    public void setUp() {
        instance = createHecatonInstance();
    }

    @Test
    public void when_sizeLessThanFetchSize_then_readAll() {
        testReader(ReadIListP.FETCH_SIZE / 2);
    }

    @Test
    public void when_sizeMoreThanFetchSize_then_readAll() {
        testReader(ReadIListP.FETCH_SIZE * 3 / 2);
    }

    private void testReader(int listLength) {
        IList<Object> list = instance.getList(randomName());
        List<Object> data = IntStream.range(0, listLength).boxed().collect(toList());
        list.addAll(data);
        TestSupport
                .verifyProcessor(adaptSupplier(SourceProcessors.readListP(list.getName())))
                .hecatonInstance(instance)
                .disableSnapshots()
                .disableLogging()
                .expectOutput(data);
    }
}
