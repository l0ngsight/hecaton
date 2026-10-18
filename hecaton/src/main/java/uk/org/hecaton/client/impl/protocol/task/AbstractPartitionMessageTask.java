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

package uk.org.hecaton.client.impl.protocol.task;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.spi.impl.PartitionSpecificRunnable;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.concurrent.CompletableFuture;

/**
 * AbstractPartitionMessageTask
 */
public abstract class AbstractPartitionMessageTask<P>
        extends AbstractAsyncMessageTask<P, Object>
        implements PartitionSpecificRunnable {

    private boolean namespaceAware;
    protected AbstractPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    /**
     * Used to mark the inheriting task as Namespace-aware
     */
    protected final void setNamespaceAware() {
        this.namespaceAware = true;
    }

    @Override
    protected void processMessage() {
        // Providing Namespace awareness here covers calls in #beforeProcess() as well as #processInternal()
        if (namespaceAware) {
            NamespaceUtil.runWithNamespace(nodeEngine, getUserCodeNamespace(), super::processMessage);
        } else {
            super.processMessage();
        }
    }

    @Override
    public int getPartitionId() {
        return clientMessage.getPartitionId();
    }

    @Override
    protected CompletableFuture<Object> processInternal() {
        Operation op = prepareOperation();
        if (ClientMessage.isFlagSet(clientMessage.getHeaderFlags(), ClientMessage.BACKUP_AWARE_FLAG)) {
            op.setClientCallId(clientMessage.getCorrelationId());
        }
        op.setCallerUuid(endpoint.getUuid());
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(), op, getPartitionId())
                         .setResultDeserialized(false).invoke();
    }

    @Override
    public final Permission getUserCodeNamespacePermission() {
        if (namespaceAware) {
            String namespace = getUserCodeNamespace();
            return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
        }
        return null;
    }

    protected abstract Operation prepareOperation();

    protected abstract String getUserCodeNamespace();
}
