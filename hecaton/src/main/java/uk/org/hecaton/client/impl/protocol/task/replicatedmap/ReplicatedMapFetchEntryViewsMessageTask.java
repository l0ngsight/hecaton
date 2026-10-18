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

package uk.org.hecaton.client.impl.protocol.task.replicatedmap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapFetchEntryViewsCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.iterator.EntryViewsWithCursor;
import uk.org.hecaton.replicatedmap.impl.operation.FetchEntryViewsOperation;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ReplicatedMapPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

public class ReplicatedMapFetchEntryViewsMessageTask
        extends AbstractPartitionMessageTask<ReplicatedMapFetchEntryViewsCodec.RequestParameters> {
    public ReplicatedMapFetchEntryViewsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new FetchEntryViewsOperation(parameters.name, parameters.partitionId, parameters.cursorId,
                parameters.batchSize, parameters.newIteration).setNodeEngine(nodeEngine);
    }

    @Override
    protected String getUserCodeNamespace() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    protected ReplicatedMapFetchEntryViewsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapFetchEntryViewsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        EntryViewsWithCursor result = (EntryViewsWithCursor) response;
        return ReplicatedMapFetchEntryViewsCodec.encodeResponse(result.getCursorId(), result.getEntryViewHolders());
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.FETCH_ENTRY_VIEWS;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.batchSize, parameters.cursorId};
    }
}
