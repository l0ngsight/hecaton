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

package uk.org.hecaton.internal.longregister.client.task;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetAndSetCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.internal.longregister.LongRegisterService;
import uk.org.hecaton.internal.longregister.operations.GetAndSetOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.AtomicLongPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

public class LongRegisterGetAndSetMessageTask
        extends AbstractPartitionMessageTask<LongRegisterGetAndSetCodec.RequestParameters> {

    public LongRegisterGetAndSetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetAndSetOperation(parameters.name, parameters.newValue);
    }

    @Override
    protected LongRegisterGetAndSetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return LongRegisterGetAndSetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return LongRegisterGetAndSetCodec.encodeResponse((Long) response);
    }

    @Override
    public String getServiceName() {
        return LongRegisterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicLongPermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "getAndSet";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.newValue};
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
