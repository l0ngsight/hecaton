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
import uk.org.hecaton.client.impl.protocol.codec.MapTryLockCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.internal.locksupport.LockSupportService;
import uk.org.hecaton.internal.locksupport.operations.LockOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class MapTryLockMessageTask
        extends AbstractPartitionMessageTask<MapTryLockCodec.RequestParameters> {

    public MapTryLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new LockOperation(getObjectNamespace(), parameters.key,
                parameters.threadId, parameters.lease, parameters.timeout, parameters.referenceId, true);
    }

    @Override
    protected MapTryLockCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapTryLockCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapTryLockCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return LockSupportService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectType() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_LOCK);
    }

    private ObjectNamespace getObjectNamespace() {
        return MapService.getObjectNamespace(parameters.name);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
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
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
