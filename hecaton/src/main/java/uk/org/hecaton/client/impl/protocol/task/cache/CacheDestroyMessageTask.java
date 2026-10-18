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

package uk.org.hecaton.client.impl.protocol.task.cache;

import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.cache.impl.operation.CacheDestroyOperation;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheDestroyCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractInvocationMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;

import java.security.Permission;

public class CacheDestroyMessageTask
        extends AbstractInvocationMessageTask<String> {

    public CacheDestroyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        OperationService operationService = nodeEngine.getOperationService();

        return operationService.createInvocationBuilder(CacheService.SERVICE_NAME, op,
                nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheDestroyOperation(parameters);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return CacheDestroyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheDestroyCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters, ActionConstants.ACTION_DESTROY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.DESTROY;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
