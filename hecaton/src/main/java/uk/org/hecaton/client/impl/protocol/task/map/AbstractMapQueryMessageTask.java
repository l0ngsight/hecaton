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

package uk.org.hecaton.client.impl.protocol.task.map;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;

import uk.org.hecaton.aggregation.Aggregator;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.task.AbstractCallableMessageTask;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.util.CollectionUtil;
import uk.org.hecaton.internal.util.IterationType;
import uk.org.hecaton.internal.util.SetUtil;
import uk.org.hecaton.internal.util.collection.PartitionIdSet;
import uk.org.hecaton.map.QueryResultSizeExceededException;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.operation.MapOperation;
import uk.org.hecaton.map.impl.query.Query;
import uk.org.hecaton.map.impl.query.Result;
import uk.org.hecaton.map.impl.query.Target;
import uk.org.hecaton.projection.Projection;
import uk.org.hecaton.query.PartitionPredicate;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.QueryException;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.IntConsumer;

public abstract class AbstractMapQueryMessageTask<P, QueryResult extends Result, AccumulatedResults, ReducedResult>
        extends AbstractCallableMessageTask<P> {

    protected AbstractMapQueryMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_READ);
    }

    protected String getUserCodeNamespace() {
        return MapService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }

    protected abstract Predicate getPredicate();

    protected abstract Aggregator<?, ?> getAggregator();

    protected abstract Projection<?, ?> getProjection();

    protected abstract void extractAndAppendResult(Collection<AccumulatedResults> results, QueryResult queryResult);

    protected abstract ReducedResult reduce(Collection<AccumulatedResults> results);

    protected abstract IterationType getIterationType();

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = getUserCodeNamespace();
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    protected final Object call() throws Exception {
        Collection<AccumulatedResults> result = new LinkedList<>();
        try {
            NamespaceUtil.setupNamespace(nodeEngine, getUserCodeNamespace());
            Predicate predicate = getPredicate();
            if (predicate instanceof PartitionPredicate partitionPredicate) {
                QueryResult queryResult = invokeOnPartitions(partitionPredicate);
                extractAndAppendResult(result, queryResult);
                return reduce(result);
            }
            int partitionCount = clientEngine.getPartitionService().getPartitionCount();

            PartitionIdSet finishedPartitions = invokeOnMembers(result, predicate, partitionCount);
            invokeOnMissingPartitions(result, predicate, finishedPartitions);
            return reduce(result);
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            NamespaceUtil.cleanupNamespace(nodeEngine, getUserCodeNamespace());
        }
    }

    private QueryResult invokeOnPartitions(PartitionPredicate predicate) {
        MapService mapService = nodeEngine.getService(getServiceName());
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        Query query = buildQuery(predicate);
        String name = query.getMapName();
        return mapServiceContext.getQueryEngine(name)
                                     .execute(query, Target.createPartitionTarget(query.getPartitionIdSet()));
    }

    private PartitionIdSet invokeOnMembers(Collection<AccumulatedResults> result, Predicate predicate, int partitionCount) {
        Collection<Member> members = clientEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        List<Future> futures = createInvocations(members, predicate);
        return collectResults(result, futures, partitionCount);
    }

    private void invokeOnMissingPartitions(Collection<AccumulatedResults> result, Predicate predicate,
                                           PartitionIdSet finishedPartitions)
            throws InterruptedException, ExecutionException {
        if (finishedPartitions.isMissingPartitions()) {
            PartitionIdSet missingPartitions = new PartitionIdSet(finishedPartitions);
            missingPartitions.complement();
            List<Future> missingFutures = new ArrayList<>(missingPartitions.size());
            createInvocationsForMissingPartitions(missingPartitions, missingFutures, predicate);
            collectResultsFromMissingPartitions(finishedPartitions, result, missingFutures);
        }
        assertAllPartitionsQueried(finishedPartitions);
    }

    private List<Future> createInvocations(Collection<Member> members, Predicate predicate) {
        List<Future> futures = new ArrayList<>(members.size());
        final OperationService operationService = nodeEngine.getOperationService();
        final Query query = buildQuery(predicate);

        MapService mapService = nodeEngine.getService(getServiceName());
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        for (Member member : members) {
            try {
                Future future = operationService.createInvocationBuilder(SERVICE_NAME,
                        createQueryOperation(query, mapServiceContext),
                        member.getAddress())
                        .invoke();
                futures.add(future);
            } catch (Throwable t) {
                if (!(t instanceof HecatonException)) {
                    // these are programmatic errors that needs to be visible
                    throw rethrow(t);
                } else if (t.getCause() instanceof QueryResultSizeExceededException) {
                    throw rethrow(t);
                } else {
                    // log failure to invoke query on member at fine level
                    // the missing partition IDs will be queried anyway, so it's not a terminal failure
                    if (logger.isFineEnabled()) {
                        logger.fine("Query invocation failed on member " + member, t);
                    }
                }
            }
        }
        return futures;
    }

    private Query buildQuery(Predicate predicate) {
        Predicate target;
        PartitionIdSet idSet;
        if (predicate instanceof PartitionPredicate partitionPredicate) {
            target = partitionPredicate.getTarget();
            idSet = nodeEngine.getPartitionService().getPartitionIdSet(partitionPredicate.getPartitionKeys());
        } else {
            target = predicate;
            idSet = SetUtil.allPartitionIds(nodeEngine.getPartitionService().getPartitionCount());
        }
        Query.QueryBuilder builder = Query.of()
                .mapName(getDistributedObjectName())
                .predicate(target)
                .partitionIdSet(idSet)
                .iterationType(getIterationType());
        if (getAggregator() != null) {
            builder = builder.aggregator(getAggregator());
        }
        if (getProjection() != null) {
            builder = builder.projection(getProjection());
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private PartitionIdSet collectResults(Collection<AccumulatedResults> result, List<Future> futures, int partitionCount) {
        PartitionIdSet finishedPartitions = new PartitionIdSet(partitionCount);
        for (Future future : futures) {
            try {
                QueryResult queryResult = (QueryResult) future.get();
                if (queryResult != null) {
                    PartitionIdSet partitionIds = queryResult.getPartitionIds();
                    if (partitionIds != null && !partitionIds.intersects(finishedPartitions)) {
                        // Collect results only if there is no overlap with already collected partitions.
                        // If there is an overlap it means there was a partition migration while QueryOperation(s) were
                        // running. In this case we discard all results from this member and will target the missing
                        // partition separately later.
                        finishedPartitions.union(partitionIds);
                        extractAndAppendResult(result, queryResult);
                    }
                }
            } catch (Throwable t) {
                if (t.getCause() instanceof QueryResultSizeExceededException) {
                    throw rethrow(t);
                } else {
                    // log failure to invoke query on member at fine level
                    // the missing partition IDs will be queried anyway, so it's not a terminal failure
                    if (logger.isFineEnabled()) {
                        logger.fine("Query on member failed with exception", t);
                    }
                }
            }
        }
        return finishedPartitions;
    }

    private void createInvocationsForMissingPartitions(PartitionIdSet missingPartitionsList, List<Future> futures,
                                                       Predicate predicate) {

        final OperationService operationService = nodeEngine.getOperationService();
        MapService mapService = nodeEngine.getService(getServiceName());
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        Query query = buildQuery(predicate);
        PrimitiveIterator.OfInt missingPartitionIterator = missingPartitionsList.intIterator();
        missingPartitionIterator.forEachRemaining((IntConsumer) partitionId -> {
            MapOperation queryPartitionOperation = createQueryPartitionOperation(query, mapServiceContext);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future future = operationService.invokeOnPartition(SERVICE_NAME,
                        queryPartitionOperation, partitionId);
                futures.add(future);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        });
    }

    private void collectResultsFromMissingPartitions(PartitionIdSet finishedPartitions, Collection<AccumulatedResults> result,
                                                     List<Future> futures)
            throws InterruptedException, ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (CollectionUtil.isNotEmpty(queryResult.getPartitionIds())
                    && !finishedPartitions.intersects(queryResult.getPartitionIds())) {
                extractAndAppendResult(result, queryResult);
                finishedPartitions.addAll(queryResult.getPartitionIds());
            }
        }
    }

    private Operation createQueryOperation(Query query, MapServiceContext mapServiceContext) {
        return mapServiceContext.getMapOperationProvider(query.getMapName()).createQueryOperation(query);
    }

    private MapOperation createQueryPartitionOperation(Query query, MapServiceContext mapServiceContext) {
        return mapServiceContext.getMapOperationProvider(
                query.getMapName()).createQueryPartitionOperation(query);
    }

    private void assertAllPartitionsQueried(PartitionIdSet finishedPartitions) {
        if (finishedPartitions.isMissingPartitions()) {
            int missedPartitionsCount = 0;
            int partitionCount = finishedPartitions.getPartitionCount();
            for (int i = 0; i < partitionCount; i++) {
                if (!finishedPartitions.contains(i)) {
                    missedPartitionsCount++;
                }
            }
            throw new QueryException("Query aborted. Could not execute query for all partitions. Missed "
                    + missedPartitionsCount + " partitions");
        }
    }

}
