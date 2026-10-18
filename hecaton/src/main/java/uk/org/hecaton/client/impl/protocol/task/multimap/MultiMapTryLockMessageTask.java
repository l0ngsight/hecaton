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
import uk.org.hecaton.client.impl.protocol.codec.MultiMapTryLockCodec;
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
public class MultiMapTryLockMessageTask
        extends AbstractPartitionMessageTask<MultiMapTryLockCodec.RequestParameters> {

    public MultiMapTryLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        DistributedObjectNamespace namespace = new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, parameters.name);
        return new LockOperation(namespace, parameters.key, parameters.threadId, parameters.lease, parameters.timeout,
                parameters.referenceId, true);
    }

    @Override
    protected MultiMapTryLockCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapTryLockCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapTryLockCodec.encodeResponse((Boolean) response);
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
        return SecurityInterceptorConstants.TRY_LOCK;
    }

    @Override
    public Object[] getParameters() {
        if (parameters.timeout == 0) {
            return new Object[]{parameters.key};
        }
        return new Object[]{parameters.key, parameters.timeout, TimeUnit.MILLISECONDS};
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
