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
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapClearCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapEventPublishingService;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.operation.ClearOperationFactory;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ReplicatedMapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

public class ReplicatedMapClearMessageTask
        extends AbstractAllPartitionsMessageTask<String> {

    public ReplicatedMapClearMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new ClearOperationFactory(parameters);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int deletedEntrySize = 0;
        for (Object deletedEntryPerPartition : map.values()) {
            deletedEntrySize += (Integer) deletedEntryPerPartition;
        }
        ReplicatedMapService service = getService(getServiceName());
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        eventPublishingService.fireMapClearedEvent(deletedEntrySize, getDistributedObjectName());
        return null;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapClearCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ReplicatedMapClearCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CLEAR;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}

