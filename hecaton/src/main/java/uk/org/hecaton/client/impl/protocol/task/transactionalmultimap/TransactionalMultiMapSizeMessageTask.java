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

package uk.org.hecaton.client.impl.protocol.task.transactionalmultimap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMultiMapSizeCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractTransactionalMessageTask;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.transaction.TransactionalMultiMap;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MultiMapPermission;
import uk.org.hecaton.transaction.TransactionContext;

import java.security.Permission;

public class TransactionalMultiMapSizeMessageTask
        extends AbstractTransactionalMessageTask<TransactionalMultiMapSizeCodec.RequestParameters> {

    public TransactionalMultiMapSizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object innerCall() throws Exception {
        final TransactionContext context = endpoint.getTransactionContext(parameters.txnId);
        TransactionalMultiMap<Object, Object> multiMap = context.getMultiMap(parameters.name);
        return multiMap.size();
    }

    @Override
    protected long getClientThreadId() {
        return parameters.threadId;
    }

    @Override
    protected TransactionalMultiMapSizeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionalMultiMapSizeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TransactionalMultiMapSizeCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_READ);
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
