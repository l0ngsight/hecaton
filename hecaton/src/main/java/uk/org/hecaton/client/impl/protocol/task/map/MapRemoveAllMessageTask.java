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
import uk.org.hecaton.client.impl.protocol.codec.MapRemoveAllCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.query.PartitionPredicate;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;
import uk.org.hecaton.spi.impl.operationservice.OperationService;

import java.security.Permission;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static uk.org.hecaton.map.impl.EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;

public class MapRemoveAllMessageTask extends AbstractMapAllPartitionsMessageTask<MapRemoveAllCodec.RequestParameters> {

    private Predicate predicate;

    public MapRemoveAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected void processMessage() {
        predicate = NamespaceUtil.callWithNamespace(nodeEngine, MapService.lookupNamespace(nodeEngine, parameters.name),
                () -> serializationService.toObject(parameters.predicate));
        if (!(predicate instanceof PartitionPredicate)) {
            super.processMessage();
            return;
        }

        NamespaceUtil.runWithNamespace(nodeEngine, getUserCodeNamespace(), () -> {
            PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
            OperationFactory operationFactory = createOperationFactory();
            OperationService operationService = nodeEngine.getOperationService();

            CompletableFuture<Map<Integer, Object>> future = operationService
                    .invokeOnPartitionsAsync(
                            getServiceName(),
                            operationFactory,
                            nodeEngine.getPartitionService().getPartitionIdSet(partitionPredicate.getPartitionKeys())
                    );
            future.whenCompleteAsync((response, throwable) -> {
                if (throwable == null) {
                    sendResponse(reduce(response));
                } else {
                    handleProcessingFailure(throwable);
                }
            }, CALLER_RUNS);
        });
    }

    @Override
    protected OperationFactory createOperationFactory() {
        MapOperationProvider operationProvider = getOperationProvider(parameters.name);
        Predicate effectivePredicate = predicate instanceof PartitionPredicate partitionPredicate ? partitionPredicate
                .getTarget() : predicate;
        return operationProvider.createPartitionWideEntryWithPredicateOperationFactory(parameters.name, ENTRY_REMOVING_PROCESSOR,
                effectivePredicate);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return null;
    }

    @Override
    protected MapRemoveAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapRemoveAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapRemoveAllCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.REMOVE_ALL;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.predicate};
    }
}
