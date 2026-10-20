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

import uk.org.hecaton.function.BiFunctionEx;
import uk.org.hecaton.jet.JetException;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.Watermark;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.jet.pipeline.ServiceFactories;
import uk.org.hecaton.jet.pipeline.ServiceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.jet.Traversers.traverseItems;
import static uk.org.hecaton.jet.Traversers.traverseIterable;
import static uk.org.hecaton.jet.impl.util.Util.exceptionallyCompletedFuture;
import static uk.org.hecaton.jet.pipeline.GeneralStage.DEFAULT_MAX_CONCURRENT_OPS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({QuickTest.class, ParallelJVMTest.class})
public class AsyncTransformUsingServiceBatchedPTest extends SimpleTestInClusterSupport {

    private ProcessorSupplier getSupplier(BiFunctionEx<? super String, ? super List<String>,
                            CompletableFuture<Traverser<String>>> mapFn
    ) {
        ServiceFactory<?, String> serviceFactory = ServiceFactories.nonSharedService(pctx -> "foo");
        return AsyncTransformUsingServiceBatchedP.supplier(serviceFactory, DEFAULT_MAX_CONCURRENT_OPS, 128, mapFn);
    }

    @BeforeClass
    public static void setUp() {
        initialize(1, null);
    }

    @Test
    public void test_completedFutures() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, items) -> completedFuture(
                        traverseIterable(items).flatMap(item -> traverseItems(item + "-1", item + "-2")))))
                .hecatonInstance(instance())
                .input(asList("a", "b"))
                .outputChecker((expected, actual) ->
                        actual.equals(asList("a-1", "a-2", "b-1", "b-2")))
                .disableProgressAssertion()
                .expectOutput(singletonList("<see outputChecker>"));
    }

    @Test
    public void test_futuresCompletedInSeparateThread() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, items) -> {
                            CompletableFuture<Traverser<String>> f = new CompletableFuture<>();
                            spawn(() -> f.complete(
                                    traverseIterable(items).flatMap(item -> traverseItems(item + "-1", item + "-2"))));
                            return f;
                        })
                )
                .hecatonInstance(instance())
                .input(asList("a", "b", new Watermark(10)))
                .outputChecker((expected, actual) ->
                        actual.equals(asList("a-1", "a-2", "b-1", "b-2", wm(10))))
                .disableProgressAssertion()
                .expectOutput(singletonList("<see code>"));
    }

    @Test
    public void test_forwardWatermarksWithoutItems() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> {
                    throw new UnsupportedOperationException();
                }))
                .hecatonInstance(instance())
                .input(singletonList(wm(10)))
                .expectOutput(singletonList(wm(10)));
    }

    @Test
    public void when_mapFnReturnsNullFuture_then_resultFilteredOut() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> null))
                .hecatonInstance(instance())
                .input(asList("a", "b"))
                .expectOutput(emptyList());
    }

    @Test
    public void when_returnedListContainsNull_then_error() {
        TestSupport testSupport = TestSupport
                .verifyProcessor(getSupplier((ctx, items) -> completedFuture(traverseItems(items).flatMap(item -> null))))
                .hecatonInstance(instance())
                .input(asList("a", "b"));

        List<Object> list = emptyList();
        assertThatThrownBy(() -> testSupport.expectOutput(list))
                .isInstanceOf(NullPointerException.class);

    }

    @Test
    public void when_futureReturnsNullTraverser_then_resultFilteredOut() {
        TestSupport
                .verifyProcessor(getSupplier((ctx, item) -> null))
                .hecatonInstance(instance())
                .input(singletonList(wm(10)))
                .expectOutput(singletonList(wm(10)));
    }

    @Test
    public void when_futureCompletedExceptionally_then_jobFails() {
        assertThatThrownBy(() ->
                TestSupport
                        .verifyProcessor(getSupplier((ctx, item) ->
                                exceptionallyCompletedFuture(new RuntimeException("test exception"))))
                        .hecatonInstance(instance())
                        .input(singletonList("a"))
                        .expectOutput(emptyList()))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("test exception");

    }
}
