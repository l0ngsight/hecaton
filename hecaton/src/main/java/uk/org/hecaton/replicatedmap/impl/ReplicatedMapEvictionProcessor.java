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

package uk.org.hecaton.replicatedmap.impl;

import uk.org.hecaton.replicatedmap.impl.operation.EvictionOperation;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.internal.util.scheduler.EntryTaskScheduler;
import uk.org.hecaton.internal.util.scheduler.ScheduledEntry;
import uk.org.hecaton.internal.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;

import static uk.org.hecaton.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Actual eviction processor implementation to remove values to evict values from the replicated map
 */
public class ReplicatedMapEvictionProcessor implements ScheduledEntryProcessor<Object, Object> {

    private ReplicatedRecordStore store;
    private NodeEngine nodeEngine;
    private int partitionId;

    public ReplicatedMapEvictionProcessor(ReplicatedRecordStore store, NodeEngine nodeEngine, int partitionId) {
        this.store = store;
        this.nodeEngine = nodeEngine;
        this.partitionId = partitionId;
    }

    public void process(EntryTaskScheduler<Object, Object> scheduler, Collection<ScheduledEntry<Object, Object>> entries) {
        EvictionOperation evictionOperation = new EvictionOperation(store, entries);
        evictionOperation.setPartitionId(partitionId);
        nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME, evictionOperation, nodeEngine.getThisAddress());
    }
}
