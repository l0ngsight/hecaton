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

package uk.org.hecaton.flakeidgen.impl;

import uk.org.hecaton.flakeidgen.FlakeIdGenerator;
import uk.org.hecaton.internal.metrics.DynamicMetricsProvider;
import uk.org.hecaton.internal.monitor.LocalFlakeIdGeneratorStats;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.internal.services.StatisticsAwareService;

public interface FlakeIdGeneratorService extends ManagedService, RemoteService,
                                                StatisticsAwareService<LocalFlakeIdGeneratorStats>, DynamicMetricsProvider {
    String SERVICE_NAME = "hz:impl:flakeIdGeneratorService";

    /**
     * Updated the statistics for the {@link FlakeIdGenerator} with the given
     * name for a newly generated batch of the given size.
     *
     * @param name name of the generator, not null
     * @param batchSize size of the batch created
     */
    void updateStatsForBatch(String name, int batchSize);
}
