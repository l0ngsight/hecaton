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

import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.ToLongFunctionEx;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.function.TriFunction;
import uk.org.hecaton.jet.function.TriPredicate;
import uk.org.hecaton.jet.impl.pipeline.PipelineImpl.Context;
import uk.org.hecaton.jet.impl.pipeline.Planner;
import uk.org.hecaton.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serial;
import java.util.function.Supplier;

import static uk.org.hecaton.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static uk.org.hecaton.jet.core.processor.Processors.flatMapStatefulP;

public class FlatMapStatefulTransform<T, K, S, R> extends StatefulKeyedTransformBase<T, K, S> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> statefulFlatMapFn;
    private final TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn;
    private final TriPredicate<? super S, ? super K, ? super T> deleteStatePredicate;

    public FlatMapStatefulTransform(
            @Nonnull Transform upstream,
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunctionEx<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nullable TriPredicate<? super S, ? super K, ? super T> deleteStatePredicate,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        super("flatmap-stateful-keyed", upstream, ttl, keyFn, timestampFn, createFn);
        this.statefulFlatMapFn = flatMapFn;
        this.onEvictFn = onEvictFn;
        this.deleteStatePredicate = deleteStatePredicate;
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(LOCAL_PARALLELISM_USE_DEFAULT, context, false);
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(),
                flatMapStatefulP(ttl, keyFn, timestampFn, createFn, statefulFlatMapFn, deleteStatePredicate, onEvictFn));
        p.addEdges(this, pv.v, edge -> edge.partitioned(keyFn).distributed());
    }
}
