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

package uk.org.hecaton.client.impl.protocol.task.transactionalmap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapSizeCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractTransactionalMessageTask;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.transaction.TransactionalMap;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.transaction.TransactionContext;

import java.security.Permission;

public class TransactionalMapSizeMessageTask
        extends AbstractTransactionalMessageTask<TransactionalMapSizeCodec.RequestParameters> {

    public TransactionalMapSizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object innerCall() throws Exception {
        final TransactionContext context = endpoint.getTransactionContext(parameters.txnId);
        final TransactionalMap map = context.getMap(parameters.name);
        return map.size();
    }

    @Override
    protected long getClientThreadId() {
        return parameters.threadId;
    }

    @Override
    protected TransactionalMapSizeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionalMapSizeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TransactionalMapSizeCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SIZE;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
