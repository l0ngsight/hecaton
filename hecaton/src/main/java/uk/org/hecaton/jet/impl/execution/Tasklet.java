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

package uk.org.hecaton.jet.impl.execution;

import uk.org.hecaton.internal.metrics.DynamicMetricsProvider;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsCollectionContext;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.impl.util.ProgressState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Tasklet extends DynamicMetricsProvider {

    default void init() {
    }

    @Nonnull
    ProgressState call();

    default boolean isCooperative() {
        return true;
    }

    default void close() {
    }

    @Nullable
    default Processor.Context getProcessorContext() {
        return null;
    }

    @Override
    default void provideDynamicMetrics(MetricDescriptor tagger, MetricsCollectionContext context) {
    }

}
