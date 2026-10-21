/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.internal.metrics.DynamicMetricsProvider;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsCollectionContext;
import uk.org.hecaton.internal.monitor.LocalWanPublisherStats;
import uk.org.hecaton.wan.WanEventCounters.DistributedObjectWanEventCounters;

import java.util.Map;
import java.util.Map.Entry;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.WAN_DISCRIMINATOR_REPLICATION;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.WAN_METRIC_DROPPED_COUNT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.WAN_METRIC_REMOVE_COUNT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.WAN_METRIC_UPDATE_COUNT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.WAN_PREFIX;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.WAN_TAG_CACHE;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.WAN_TAG_MAP;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.WAN_TAG_PUBLISHERID;
import static uk.org.hecaton.internal.metrics.ProbeLevel.INFO;
import static uk.org.hecaton.internal.metrics.ProbeUnit.COUNT;

/**
 * Metrics of the WAN publishers: {@code wan[<scheme>].<metric>} tagged with the publisher ID, and
 * the sent event counts of each map and cache.
 */
class WanMetricsProvider implements DynamicMetricsProvider {

    private final Map<String, DelegatingWanScheme> wanReplications;

    WanMetricsProvider(Map<String, DelegatingWanScheme> wanReplications) {
        this.wanReplications = wanReplications;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        for (Entry<String, DelegatingWanScheme> scheme : wanReplications.entrySet()) {
            for (Entry<String, LocalWanPublisherStats> publisher : scheme.getValue().getStats().entrySet()) {
                MetricDescriptor publisherDescriptor = descriptor.copy().withPrefix(WAN_PREFIX)
                        .withDiscriminator(WAN_DISCRIMINATOR_REPLICATION, scheme.getKey())
                        .withTag(WAN_TAG_PUBLISHERID, publisher.getKey());
                LocalWanPublisherStats stats = publisher.getValue();
                context.collect(publisherDescriptor, stats);
                provideCounterMetrics(publisherDescriptor, WAN_TAG_MAP, stats.getSentMapEventCounter(), context);
                provideCounterMetrics(publisherDescriptor, WAN_TAG_CACHE, stats.getSentCacheEventCounter(), context);
            }
        }
    }

    private static void provideCounterMetrics(MetricDescriptor publisherDescriptor, String tag,
                                              Map<String, DistributedObjectWanEventCounters> counters,
                                              MetricsCollectionContext context) {
        if (counters == null) {
            return;
        }
        for (Entry<String, DistributedObjectWanEventCounters> counter : counters.entrySet()) {
            MetricDescriptor counterDescriptor = publisherDescriptor.copy().withTag(tag, counter.getKey());
            DistributedObjectWanEventCounters value = counter.getValue();
            context.collect(counterDescriptor, WAN_METRIC_UPDATE_COUNT, INFO, COUNT, value.getUpdateCount());
            context.collect(counterDescriptor, WAN_METRIC_REMOVE_COUNT, INFO, COUNT, value.getRemoveCount());
            context.collect(counterDescriptor, WAN_METRIC_DROPPED_COUNT, INFO, COUNT, value.getDroppedCount());
        }
    }
}
