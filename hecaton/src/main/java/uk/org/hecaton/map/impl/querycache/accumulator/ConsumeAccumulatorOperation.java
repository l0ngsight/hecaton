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

package uk.org.hecaton.map.impl.querycache.accumulator;

import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.QueryCacheEventService;
import uk.org.hecaton.map.impl.querycache.event.sequence.Sequenced;
import uk.org.hecaton.map.impl.querycache.publisher.EventPublisherAccumulatorProcessor;
import uk.org.hecaton.map.impl.querycache.publisher.MapPublisherRegistry;
import uk.org.hecaton.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherAccumulatorHandler;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherContext;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherRegistry;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.Preconditions.checkPositive;

/**
 * Processes remaining items in {@link Accumulator} instances on a partition.
 * If the partition is not owned by this node, {@link Accumulator} will be removed.
 */
public class ConsumeAccumulatorOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    private int maxProcessableAccumulatorCount;
    private Queue<Accumulator> accumulators;

    public ConsumeAccumulatorOperation() {
    }

    public ConsumeAccumulatorOperation(Queue<Accumulator> accumulators, int maxProcessableAccumulatorCount) {
        checkPositive("maxProcessableAccumulatorCount", maxProcessableAccumulatorCount);

        this.accumulators = accumulators;
        this.maxProcessableAccumulatorCount = maxProcessableAccumulatorCount;
    }

    @Override
    public void run() throws Exception {
        QueryCacheContext context = getQueryCacheContext();

        QueryCacheEventService queryCacheEventService = context.getQueryCacheEventService();
        EventPublisherAccumulatorProcessor processor = new EventPublisherAccumulatorProcessor(queryCacheEventService);
        AccumulatorHandler<Sequenced> handler = new PublisherAccumulatorHandler(context, processor);

        int processed = 0;
        do {
            Accumulator accumulator = accumulators.poll();
            if (accumulator == null) {
                break;
            }

            if (isLocal()) {
                // consume the accumulator if only this node is the owner
                // of accumulators partition
                publishAccumulator(processor, handler, accumulator);
            } else {
                // if the accumulator is not local, it should be a leftover
                // stayed after partition migrations and remove that accumulator
                removeAccumulator(context, accumulator);
            }
            processed++;

        } while (processed <= maxProcessableAccumulatorCount);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    private void publishAccumulator(EventPublisherAccumulatorProcessor processor,
                                    AccumulatorHandler<Sequenced> handler, Accumulator accumulator) {
        AccumulatorInfo info = accumulator.getInfo();
        processor.setInfo(info);
        accumulator.poll(handler, info.getDelaySeconds(), TimeUnit.SECONDS);
    }

    private QueryCacheContext getQueryCacheContext() {
        MapService mapService = getService();
        return mapService.getMapServiceContext().getQueryCacheContext();
    }

    private boolean isLocal() {
        NodeEngine nodeEngine = getNodeEngine();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        IPartition partition = partitionService.getPartition(getPartitionId());
        return partition.isLocal();
    }

    private void removeAccumulator(QueryCacheContext context, Accumulator accumulator) {
        PublisherContext publisherContext = context.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        AccumulatorInfo info = accumulator.getInfo();
        String mapName = info.getMapName();
        String cacheName = info.getCacheId();

        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(mapName);
        if (publisherRegistry == null) {
            return;
        }

        PartitionAccumulatorRegistry partitionAccumulatorRegistry = publisherRegistry.getOrNull(cacheName);
        if (partitionAccumulatorRegistry == null) {
            return;
        }

        partitionAccumulatorRegistry.remove(getPartitionId());
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.ACCUMULATOR_CONSUMER;
    }
}
