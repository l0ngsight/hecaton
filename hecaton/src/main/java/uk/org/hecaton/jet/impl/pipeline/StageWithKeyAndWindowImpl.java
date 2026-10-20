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

package uk.org.hecaton.jet.impl.pipeline;

import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.aggregate.AggregateOperation1;
import uk.org.hecaton.jet.aggregate.AggregateOperation2;
import uk.org.hecaton.jet.aggregate.AggregateOperation3;
import uk.org.hecaton.jet.datamodel.KeyedWindowResult;
import uk.org.hecaton.jet.impl.pipeline.transform.Transform;
import uk.org.hecaton.jet.impl.pipeline.transform.WindowGroupTransform;
import uk.org.hecaton.jet.pipeline.GeneralStage;
import uk.org.hecaton.jet.pipeline.StageWithKeyAndWindow;
import uk.org.hecaton.jet.pipeline.StreamStage;
import uk.org.hecaton.jet.pipeline.StreamStageWithKey;
import uk.org.hecaton.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;

import static uk.org.hecaton.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static uk.org.hecaton.jet.impl.pipeline.ComputeStageImplBase.ensureJetEvents;
import static uk.org.hecaton.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation2;
import static uk.org.hecaton.jet.impl.pipeline.JetEventFunctionAdapter.adaptAggregateOperation3;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class StageWithKeyAndWindowImpl<T, K>
        extends StageWithGroupingBase<T, K>
        implements StageWithKeyAndWindow<T, K> {

    @Nonnull
    private final WindowDefinition wDef;

    StageWithKeyAndWindowImpl(
            @Nonnull StreamStageImpl<T> computeStage,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull WindowDefinition wDef
    ) {
        super(computeStage, keyFn);
        this.wDef = wDef;
    }

    @Nonnull @Override
    public WindowDefinition windowDefinition() {
        return wDef;
    }

    @Nonnull @Override
    public <R> StreamStage<KeyedWindowResult<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        FunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, R>(
                        singletonList(computeStage.transform),
                        wDef,
                        singletonList(fnAdapter.adaptKeyFn(keyFn())),
                        fnAdapter.adaptAggregateOperation1(aggrOp)
                ),
                fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("rawtypes")
    public <T1, R> StreamStage<KeyedWindowResult<K, R>> aggregate2(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp
    ) {
        ensureJetEvents(computeStage, "This pipeline stage");
        ComputeStageImplBase computeStage1 = ((StageWithGroupingBase) stage1).computeStage;
        ensureJetEvents(computeStage1, "stage1");
        Transform upstream1 = computeStage1.transform;
        FunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return this.computeStage.attach(new WindowGroupTransform<K, R>(
                        asList(this.computeStage.transform, upstream1),
                        wDef,
                        asList(fnAdapter.adaptKeyFn(keyFn()),
                                fnAdapter.adaptKeyFn(stage1.keyFn())),
                        adaptAggregateOperation2(aggrOp)
                ),
                singletonList((GeneralStage<?>) computeStage1),
                fnAdapter);
    }

    @Nonnull @Override
    @SuppressWarnings("rawtypes")
    public <T1, T2, R> StreamStage<KeyedWindowResult<K, R>> aggregate3(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull StreamStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    ) {
        ComputeStageImplBase computeStage1 = ((StageWithGroupingBase) stage1).computeStage;
        ComputeStageImplBase computeStage2 = ((StageWithGroupingBase) stage2).computeStage;
        ensureJetEvents(computeStage, "This pipeline stage");
        ensureJetEvents(computeStage1, "stage1");
        ensureJetEvents(computeStage2, "stage2");
        Transform transform1 = ((StageWithGroupingBase) stage1).computeStage.transform;
        Transform transform2 = ((StageWithGroupingBase) stage2).computeStage.transform;
        FunctionAdapter fnAdapter = ADAPT_TO_JET_EVENT;
        return computeStage.attach(new WindowGroupTransform<K, R>(
                        asList(computeStage.transform, transform1, transform2),
                        wDef,
                        asList(fnAdapter.adaptKeyFn(keyFn()),
                                fnAdapter.adaptKeyFn(stage1.keyFn()),
                                fnAdapter.adaptKeyFn(stage2.keyFn())),
                        adaptAggregateOperation3(aggrOp)
                ),
                asList((GeneralStage<?>) computeStage1, (GeneralStage<?>) computeStage2),
                fnAdapter);
    }
}
