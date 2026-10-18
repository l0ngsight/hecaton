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

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractCallableMessageTask;
import uk.org.hecaton.client.impl.protocol.task.BlockingMessageTask;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.internal.util.collection.InflatableSet;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.query.QueryResult;
import uk.org.hecaton.map.impl.query.QueryResultRow;
import uk.org.hecaton.map.impl.querycache.accumulator.AccumulatorInfo;
import uk.org.hecaton.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.OperationService;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec#REQUEST_MESSAGE_TYPE}
 */
public class MapPublisherCreateMessageTask
        extends AbstractCallableMessageTask<ContinuousQueryPublisherCreateCodec.RequestParameters>
        implements BlockingMessageTask {

    public MapPublisherCreateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        ClusterService clusterService = clientEngine.getClusterService();
        Collection<MemberImpl> members = clusterService.getMemberImpls();
        List<Future> futures = new ArrayList<>(members.size());
        NamespaceUtil.runWithNamespace(nodeEngine,
                MapService.lookupNamespace(nodeEngine, parameters.mapName),
                () -> createInvocations(members, futures));

        return fetchMapSnapshotFrom(futures);
    }

    private void createInvocations(Collection<MemberImpl> members, List<Future> futures) {
        final OperationService operationService = nodeEngine.getOperationService();
        for (MemberImpl member : members) {
            Predicate predicate = serializationService.toObject(parameters.predicate);
            AccumulatorInfo accumulatorInfo =
                    AccumulatorInfo.toAccumulatorInfo(parameters.mapName, parameters.cacheName, predicate,
                            parameters.batchSize, parameters.bufferSize, parameters.delaySeconds,
                            false, parameters.populate, parameters.coalesce);


            PublisherCreateOperation operation = new PublisherCreateOperation(accumulatorInfo);
            operation.setCallerUuid(endpoint.getUuid());
            Address address = member.getAddress();
            InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, address);
            Future future = invocationBuilder.invoke();
            futures.add(future);
        }
    }

    private static Set<Data> fetchMapSnapshotFrom(List<Future> futures) {
        List<Object> queryResults = new ArrayList<>(futures.size());
        int queryResultSize = 0;

        for (Future future : futures) {
            Object result;
            try {
                result = future.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
            if (result == null) {
                continue;
            }

            queryResults.add(result);
            queryResultSize += ((QueryResult) result).size();
        }

        return unpackResults(queryResults, queryResultSize);
    }

    private static Set<Data> unpackResults(List<Object> results, int numOfEntries) {
        InflatableSet.Builder<Data> builder = InflatableSet.newBuilder(numOfEntries);
        for (Object result : results) {
            for (QueryResultRow row : (QueryResult) result) {
                builder.add(row.getKey());
            }
        }
        return builder.build();
    }

    @Override
    protected ContinuousQueryPublisherCreateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ContinuousQueryPublisherCreateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ContinuousQueryPublisherCreateCodec.encodeResponse((Set<Data>) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.mapName, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
