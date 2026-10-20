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

package uk.org.hecaton.jet.impl.pipeline.transform;

import uk.org.hecaton.function.BiFunctionEx;
import uk.org.hecaton.function.BiPredicateEx;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.core.Edge;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.impl.pipeline.PipelineImpl.Context;
import uk.org.hecaton.jet.impl.pipeline.Planner;
import uk.org.hecaton.jet.impl.pipeline.Planner.PlannerVertex;
import uk.org.hecaton.jet.impl.processor.AsyncTransformUsingServiceBatchedP;
import uk.org.hecaton.jet.impl.processor.AsyncTransformUsingServiceOrderedP;
import uk.org.hecaton.jet.impl.processor.AsyncTransformUsingServiceUnorderedP;
import uk.org.hecaton.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static uk.org.hecaton.jet.core.processor.Processors.filterUsingServiceP;
import static uk.org.hecaton.jet.core.processor.Processors.flatMapUsingServiceP;
import static uk.org.hecaton.jet.core.processor.Processors.mapUsingServiceP;

public class ProcessorTransform extends AbstractTransform {
    public static final int NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM = 2;

    @Serial
    private static final long serialVersionUID = 1L;

    final ProcessorMetaSupplier processorSupplier;

    ProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier processorSupplier
    ) {
        super(name, upstream);
        this.processorSupplier = processorSupplier;
    }

    public static ProcessorTransform customProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier createProcessorFn
    ) {
        return new ProcessorTransform(name, upstream, createProcessorFn);
    }

    public static <S, T, R> ProcessorTransform mapUsingServiceTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return new ProcessorTransform("mapUsingService", upstream,
                ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), serviceFactory.permission(),
                        mapUsingServiceP(serviceFactory, mapFn)));
    }

    public static <S, T> ProcessorTransform filterUsingServiceTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return new ProcessorTransform("filterUsingService", upstream,
                ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), serviceFactory.permission(),
                        filterUsingServiceP(serviceFactory, filterFn)));
    }

    public static <S, T, R> ProcessorTransform flatMapUsingServiceTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return new ProcessorTransform("flatMapUsingService", upstream,
                ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), serviceFactory.permission(),
                        flatMapUsingServiceP(serviceFactory, flatMapFn)));
    }

    public static <S, T, R> ProcessorTransform flatMapUsingServiceAsyncTransform(
            @Nonnull Transform upstream,
            @Nonnull String operationName,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        // TODO use better key so that snapshots are local. Currently they will
        //      be sent to a random member. We keep it this way for simplicity:
        //      the number of in-flight items is limited (maxConcurrentOps)
        ProcessorSupplier supplier = flatMapUsingServiceAsyncP(
                serviceFactory, maxConcurrentOps, preserveOrder, Object::hashCode, flatMapAsyncFn);
        ProcessorMetaSupplier metaSupplier = ProcessorMetaSupplier.of(getPreferredLP(serviceFactory),
                serviceFactory.permission(), supplier);
        return new ProcessorTransform(operationName + "UsingServiceAsync", upstream, metaSupplier);
    }

    public static <S, T, R> ProcessorTransform flatMapUsingServiceAsyncBatchedTransform(
            @Nonnull Transform upstream,
            @Nonnull String operationName,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        String name = operationName + "UsingServiceAsyncBatched";
        ProcessorSupplier supplier = flatMapUsingServiceAsyncBatchedP(
                serviceFactory, maxConcurrentOps, maxBatchSize, flatMapAsyncFn);
        ProcessorMetaSupplier metaSupplier = ProcessorMetaSupplier.of(getPreferredLP(serviceFactory),
                serviceFactory.permission(), supplier);
        return new ProcessorTransform(name, upstream, metaSupplier);
    }

    static int getPreferredLP(@Nonnull ServiceFactory<?, ?> serviceFactory) {
        return serviceFactory.isCooperative()
                ? LOCAL_PARALLELISM_USE_DEFAULT
                : NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM;
    }

    static <C, S, T, K, R> ProcessorSupplier flatMapUsingServiceAsyncP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull FunctionEx<? super T, ? extends K> extractKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        return preserveOrder
                ? AsyncTransformUsingServiceOrderedP.supplier(
                        serviceFactory, maxConcurrentOps, flatMapAsyncFn)
                : AsyncTransformUsingServiceUnorderedP.supplier(
                        serviceFactory, maxConcurrentOps, flatMapAsyncFn, extractKeyFn);
    }

    static <C, S, T, R> ProcessorSupplier flatMapUsingServiceAsyncBatchedP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            int maxConcurrentOps,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        return AsyncTransformUsingServiceBatchedP.supplier(serviceFactory, maxConcurrentOps, maxBatchSize, flatMapAsyncFn);
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(processorSupplier.preferredLocalParallelism(), context, p.isPreserveOrder());
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(), processorSupplier);
        if (p.isPreserveOrder()) {
            p.addEdges(this, pv.v, Edge::isolated);
        } else {
            p.addEdges(this, pv.v);
        }
    }
}
