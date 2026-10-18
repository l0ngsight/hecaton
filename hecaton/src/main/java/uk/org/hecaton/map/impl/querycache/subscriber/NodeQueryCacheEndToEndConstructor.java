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

package uk.org.hecaton.map.impl.querycache.subscriber;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.map.impl.query.QueryResult;
import uk.org.hecaton.map.impl.querycache.InvokerWrapper;
import uk.org.hecaton.map.impl.querycache.accumulator.AccumulatorInfo;
import uk.org.hecaton.map.impl.querycache.subscriber.operation.MadePublishableOperation;
import uk.org.hecaton.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import static uk.org.hecaton.internal.util.CollectionUtil.isEmpty;
import static uk.org.hecaton.internal.util.FutureUtil.returnWithDeadline;
import static uk.org.hecaton.internal.util.FutureUtil.waitWithDeadline;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Node-side implementation of {@code QueryCacheEndToEndConstructor}.
 *
 * @see QueryCacheEndToEndConstructor
 */
public class NodeQueryCacheEndToEndConstructor extends AbstractQueryCacheEndToEndConstructor {

    public NodeQueryCacheEndToEndConstructor(QueryCacheRequest request) {
        super(request);
    }

    @Override
    public void createPublisherAccumulator(AccumulatorInfo info, boolean urgent) {
        // create publishers and execute initial population query in one go
        Collection<QueryResult> results = createPublishersAndGetQueryResults(info);
        if (!isEmpty(results)) {
            prepopulate(queryCache, results);
        }

        boolean populate = info.isPopulate();

        if (logger.isFinestEnabled()) {
            logger.finest("Pre population is %s", populate ? "enabled" : "disabled");
        }

        if (populate) {
            madePublishable(info.getMapName(), info.getCacheId());
        }
    }

    private Collection<QueryResult> createPublishersAndGetQueryResults(AccumulatorInfo info) {
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        Collection<Member> members = context.getMemberList();

        List<Future<QueryResult>> futures = new ArrayList<>(members.size());
        for (Member member : members) {
            Future future = invokerWrapper.invokeOnTarget(new PublisherCreateOperation(info), member);
            futures.add(future);
        }
        return returnWithDeadline(futures, OPERATION_WAIT_TIMEOUT_MINUTES, MINUTES);
    }

    private void madePublishable(String mapName, String cacheId) {
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();

        Collection<Member> memberList = context.getMemberList();
        List<Future> futures = new ArrayList<>(memberList.size());
        for (Member member : memberList) {
            Operation operation = new MadePublishableOperation(mapName, cacheId);
            Future future = invokerWrapper.invokeOnTarget(operation, member);
            futures.add(future);
        }

        waitWithDeadline(futures, OPERATION_WAIT_TIMEOUT_MINUTES, MINUTES);
    }

    private static void prepopulate(InternalQueryCache queryCache, Collection<QueryResult> resultSets) {
        for (QueryResult queryResult : resultSets) {
            try {
                if (queryResult == null || queryResult.isEmpty()) {
                    continue;
                }

                if (queryCache.reachedMaxCapacity()) {
                    break;
                }

                queryCache.prepopulate(queryResult.iterator());
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }
}
