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

package uk.org.hecaton.client.impl.protocol.task.multimap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapLockCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.internal.locksupport.LockSupportService;
import uk.org.hecaton.internal.locksupport.operations.LockOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MultiMapPermission;
import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client Protocol Task for handling messages with type ID:
 */
public class MultiMapLockMessageTask
        extends AbstractPartitionMessageTask<MultiMapLockCodec.RequestParameters> {

    public MultiMapLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        DistributedObjectNamespace namespace = getObjectNamespace();
        return new LockOperation(namespace, parameters.key, parameters.threadId, parameters.ttl,
                -1, parameters.referenceId, true);
    }

    private DistributedObjectNamespace getObjectNamespace() {
        return new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, parameters.name);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapLockCodec.encodeResponse();
    }

    @Override
    protected MultiMapLockCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapLockCodec.decodeRequest(clientMessage);
    }

    @Override
    public String getServiceName() {
        return LockSupportService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectType() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.LOCK;
    }

    @Override
    public Object[] getParameters() {
        if (parameters.ttl == -1) {
            return new Object[]{parameters.key};
        }
        return new Object[]{parameters.key, parameters.ttl, TimeUnit.MILLISECONDS};
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
