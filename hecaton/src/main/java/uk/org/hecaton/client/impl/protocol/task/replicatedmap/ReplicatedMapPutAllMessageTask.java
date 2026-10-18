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
import uk.org.hecaton.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.operation.PutAllOperationFactory;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ReplicatedMapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

import static uk.org.hecaton.internal.util.MapUtil.createHashMap;

public class ReplicatedMapPutAllMessageTask
        extends AbstractAllPartitionsMessageTask<ReplicatedMapPutAllCodec.RequestParameters> {

    public ReplicatedMapPutAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new PutAllOperationFactory(parameters.name, new MapEntries(parameters.entries));
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            Object result = serializationService.toObject(entry.getValue());
            if (result instanceof Throwable throwable) {
                throw ExceptionUtil.rethrow(throwable);
            }
        }
        return null;
    }

    @Override
    protected ReplicatedMapPutAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapPutAllCodec.decodeRequest(clientMessage);
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
        return SecurityInterceptorConstants.PUT_ALL;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public Object[] getParameters() {
        final Map map = createHashMap(parameters.entries.size());
        for (Map.Entry entry : parameters.entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return new Object[]{map};
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}

