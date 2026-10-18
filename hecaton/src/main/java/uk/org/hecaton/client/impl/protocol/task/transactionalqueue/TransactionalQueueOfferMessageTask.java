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

package uk.org.hecaton.client.impl.protocol.task.transactionalqueue;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalQueueOfferCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractTransactionalMessageTask;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.transaction.TransactionalQueue;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.QueuePermission;
import uk.org.hecaton.transaction.TransactionContext;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class TransactionalQueueOfferMessageTask
        extends AbstractTransactionalMessageTask<TransactionalQueueOfferCodec.RequestParameters> {

    public TransactionalQueueOfferMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object innerCall() throws Exception {
        final TransactionContext context = endpoint.getTransactionContext(parameters.txnId);
        final TransactionalQueue queue = context.getQueue(parameters.name);
        return queue.offer(parameters.item, parameters.timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    protected long getClientThreadId() {
        return parameters.threadId;
    }

    @Override
    protected TransactionalQueueOfferCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionalQueueOfferCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TransactionalQueueOfferCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_ADD);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.OFFER;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.item, parameters.timeout, TimeUnit.MILLISECONDS};
    }
}
