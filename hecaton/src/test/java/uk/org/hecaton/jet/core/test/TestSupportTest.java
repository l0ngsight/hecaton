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

package uk.org.hecaton.jet.core.test;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.TestProcessors.MockP;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static uk.org.hecaton.jet.core.processor.Processors.noopP;
import static uk.org.hecaton.jet.core.test.TestSupport.SAME_ITEMS_ANY_ORDER;
import static uk.org.hecaton.jet.core.test.TestSupport.verifyProcessor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TestSupportTest {

    @Test
    public void test_compareAsSet() {
        assertTrue(SAME_ITEMS_ANY_ORDER.test(asList("a", "b", "a"), asList("a", "a", "b")));
        assertFalse(SAME_ITEMS_ANY_ORDER.test(asList("a", "b", "a"), asList("a", "b", "b")));
    }

    @Test
    public void when_processorSupplierTested_then_completeCalled() {
        boolean[] completeCalled = {false};

        ProcessorSupplier supplier = new ProcessorSupplier() {
            @Nonnull
            @Override
            public Collection<? extends Processor> get(int count) {
                assertEquals(1, count);
                return singletonList(noopP().get());
            }

            @Override
            public void close(Throwable error) {
                completeCalled[0] = true;
            }
        };

        TestSupport
                .verifyProcessor(supplier)
                .expectOutput(emptyList());

        assertTrue("PS.complete not called", completeCalled[0]);

        // test once more with PMS
        completeCalled[0] = false;
        TestSupport
                .verifyProcessor(ProcessorMetaSupplier.of(supplier))
                .expectOutput(emptyList());

        assertTrue("PS.complete not called when using PMS", completeCalled[0]);
    }

    @Test
    public void test_processorMetaSupplierHasJetInstance() {
        HecatonInstance hecatonInstance = mockHecatonInstance();
        boolean[] called = {false};

        verifyProcessor(
                new ProcessorMetaSupplier() {
                    @Override
                    public void init(@Nonnull Context context) {
                        assertSame(context.hecatonInstance(), hecatonInstance);
                        called[0] = true;
                    }

                    @Nonnull
                    @Override
                    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
                        return a -> ProcessorSupplier.of(MockP::new);
                    }
                })
                .hecatonInstance(hecatonInstance)
                .expectOutput(emptyList());

        assertTrue(called[0]);
    }

    @Test
    public void test_processorSupplierHasJetInstance() {
        HecatonInstance hecatonInstance = mockHecatonInstance();

        boolean[] called = {false};

        verifyProcessor(
                new ProcessorSupplier() {
                    @Override
                    public void init(@Nonnull Context context) {
                        assertSame(context.hecatonInstance(), hecatonInstance);
                        called[0] = true;
                    }

                    @Nonnull
                    @Override
                    public Collection<? extends Processor> get(int count) {
                        assertEquals(1, count);
                        return singletonList(new MockP());
                    }
                })
                .hecatonInstance(hecatonInstance)
                .expectOutput(emptyList());

        assertTrue(called[0]);
    }

    private HecatonInstance mockHecatonInstance() {
        HecatonInstance hzInstance = mock(HecatonInstance.class);
        Cluster cluster = mock(Cluster.class);
        Member localMember = mock(Member.class);

        when(hzInstance.getCluster()).thenReturn(cluster);
        when(cluster.getLocalMember()).thenReturn(localMember);
        return hzInstance;
    }
}
