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

package uk.org.hecaton.internal.metrics.impl;

import uk.org.hecaton.instance.LocalInstanceStats;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsCollectionContext;

import java.util.Map;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.GENERAL_DISCRIMINATOR_NAME;

public final class ProviderHelper {
    private ProviderHelper() {
    }

    public static void provide(MetricDescriptor descriptor, MetricsCollectionContext context, String prefix,
                               Map<String, ? extends LocalInstanceStats> stats) {
        if (stats == null) {
            return;
        }

        for (Map.Entry<String, ? extends LocalInstanceStats> entry : stats.entrySet()) {
            String name = entry.getKey();
            LocalInstanceStats localStats = entry.getValue();

            MetricDescriptor dsDescriptor = descriptor
                    .copy()
                    .withPrefix(prefix)
                    .withDiscriminator(GENERAL_DISCRIMINATOR_NAME, name);
            context.collect(dsDescriptor, localStats);
        }
    }
}
