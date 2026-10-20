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

package uk.org.hecaton.client.impl.protocol.task.transaction;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionCommitCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractCallableMessageTask;
import uk.org.hecaton.client.impl.protocol.task.TransactionalMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.TransactionPermission;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.impl.Transaction;
import uk.org.hecaton.transaction.impl.xa.TransactionAccessor;
import uk.org.hecaton.transaction.impl.xa.XAService;

import java.security.Permission;
import java.util.UUID;

public class XATransactionCommitMessageTask
        extends AbstractCallableMessageTask<XATransactionCommitCodec.RequestParameters>
        implements TransactionalMessageTask {
    public XATransactionCommitMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected XATransactionCommitCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return XATransactionCommitCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return XATransactionCommitCodec.encodeResponse();
    }

    @Override
    protected Object call() throws Exception {
        UUID transactionId = parameters.transactionId;
        TransactionContext transactionContext = endpoint.getTransactionContext(transactionId);
        if (transactionContext == null) {
            throw new TransactionException("No transaction context with given transactionId: " + transactionId);
        }
        Transaction transaction = TransactionAccessor.getTransaction(transactionContext);
        if (parameters.onePhase) {
            transaction.prepare();
        }
        transaction.commit();
        endpoint.removeTransactionContext(transactionId);
        return null;
    }

    @Override
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.COMMIT;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
