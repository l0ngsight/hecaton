/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.sql.impl.connector.map;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.BiFunctionEx;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.util.PartitioningStrategyUtil;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.impl.connector.HecatonReaders.LocalMapReaderFunction;
import uk.org.hecaton.jet.impl.connector.ReadMapOrCacheP.LocalProcessorMetaSupplier;
import uk.org.hecaton.jet.impl.connector.ReadMapOrCacheP.LocalProcessorSupplier;
import uk.org.hecaton.jet.impl.connector.ReadMapOrCacheP.Reader;
import uk.org.hecaton.jet.impl.util.FixedCapacityIntArrayList;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static uk.org.hecaton.security.permission.ActionConstants.ACTION_CREATE;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_READ;

/**
 * A meta-supplier that will compute required partitions to scan,
 * if attribute partitioning strategy was applied.
 */
public abstract class SpecificPartitionsImapReaderPms<F extends CompletableFuture, B, R>
        extends LocalProcessorMetaSupplier<F, B, R> {
    // visible for tests
    transient int[] partitionsToScan;
    private final List<List<Expression<?>>> requiredPartitionsExprs;
    private final PartitioningStrategy<?> partitioningStrategy;
    private transient Map<Address, int[]> partitionAssignment;

    private SpecificPartitionsImapReaderPms(
            final BiFunctionEx<HecatonInstance, InternalSerializationService, Reader<F, B, R>> readerSupplier,
            @Nullable PartitioningStrategy<?> partitioningStrategy,
            @Nullable final List<List<Expression<?>>> requiredPartitionsExprs) {
        super(readerSupplier);
        this.partitioningStrategy = partitioningStrategy;
        this.requiredPartitionsExprs = requiredPartitionsExprs;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        if (requiredPartitionsExprs != null) {
            FixedCapacityIntArrayList partitionsToScanList = new FixedCapacityIntArrayList(requiredPartitionsExprs.size());

            HecatonInstance hecatonInstance = context.hecatonInstance();
            ExpressionEvalContext eec = ExpressionEvalContext.from(context);
            for (List<Expression<?>> requiredPartitionsExpr : requiredPartitionsExprs) {
                Object[] partitionKeyComponents = new Object[requiredPartitionsExpr.size()];
                int i = 0;
                for (Expression<?> expression : requiredPartitionsExpr) {
                    partitionKeyComponents[i++] = expression.evalTop(null, eec);
                }

                final Integer partitionId = PartitioningStrategyUtil.getPartitionIdFromKeyComponents(
                        Util.getNodeEngine(hecatonInstance), partitioningStrategy, partitionKeyComponents);
                if (partitionId == null) {
                    // The produced partitioning key is somehow invalid, most likely null.
                    // In this case we revert to non-pruning logic.
                    return;
                }
                assert context.partitionAssignment().values().stream()
                        .anyMatch(pa -> Arrays.binarySearch(pa, partitionId) >= 0)
                        : "Partition calculated for PMS is not present in the job";
                partitionsToScanList.add(partitionId);
            }

            // requiredPartitionsExprs may produce the same partition multiple times
            // partitionsToScan is required to be unique and sorted
            partitionsToScan = partitionsToScanList.stream().sorted().distinct().toArray();
            partitionAssignment = context.partitionAssignment();
        }
    }

    @Nonnull
    @Override
    public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        if (partitionsToScan == null) {
            return address -> new LocalProcessorSupplier<>(readerSupplier);
        } else {
            return address -> {
                int[] partitions = partitionAssignment.get(address);
                List<Integer> partitionsToScanList = new ArrayList<>();
                // partitionAssignment is sorted, so we can use binary search
                for (int pId : partitionsToScan) {
                    if (Arrays.binarySearch(partitions, pId) >= 0) {
                        partitionsToScanList.add(pId);
                    }
                }

                int[] memberPartitionsToScan = partitionsToScanList.stream().mapToInt(i -> i).toArray();
                return new LocalProcessorSupplier<>(readerSupplier, memberPartitionsToScan);
            };
        }
    }

    @Override
    public boolean isReusable() {
        return requiredPartitionsExprs == null;
    }

    @Override
    public boolean initIsCooperative() {
        return requiredPartitionsExprs == null ?
                super.initIsCooperative() :
                requiredPartitionsExprs.stream().allMatch(l -> l.stream().allMatch(Expression::isCooperative));
    }

    // TODO: name it properly.
    public static ProcessorMetaSupplier mapReader(String mapName,
                                                  @Nullable PartitioningStrategy<?> partitioningStrategy,
                                                  @Nullable List<List<Expression<?>>> requiredPartitionsExprs) {
        return new SpecificPartitionsImapReaderPms<>(new LocalMapReaderFunction(mapName),
                partitioningStrategy, requiredPartitionsExprs) {
            @Override
            public Permission getRequiredPermission() {
                return new MapPermission(mapName, ACTION_CREATE, ACTION_READ);
            }
        };
    }
}
