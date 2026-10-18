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
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapPutAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapPutAllWithMetadataCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.operation.PutAllWithMetadataOperation;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ReplicatedMapPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

public class ReplicatedMapPutAllWithMetadataMessageTask
        extends AbstractPartitionMessageTask<ReplicatedMapPutAllWithMetadataCodec.RequestParameters> {

    public ReplicatedMapPutAllWithMetadataMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new PutAllWithMetadataOperation(parameters.name, parameters.entries, parameters.partitionId)
                .setServiceName(ReplicatedMapService.SERVICE_NAME);
    }

    @Override
    protected String getUserCodeNamespace() {
        return null;
    }

    @Override
    protected ReplicatedMapPutAllWithMetadataCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapPutAllWithMetadataCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ReplicatedMapPutAllCodec.encodeResponse();
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
        return SecurityInterceptorConstants.PUT_ALL_WITH_METADATA;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.name, parameters.entries, parameters.partitionId};
    }
}

