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
import uk.org.hecaton.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.partition.PartitionLostListener;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class AddPartitionLostListenerMessageTask
        extends AbstractAddListenerMessageTask<Boolean> {

    public AddPartitionLostListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        final IPartitionService partitionService = getService(getServiceName());

        final PartitionLostListener listener = event -> {
            if (endpoint.isAlive()) {
                ClusterService clusterService = getService(ClusterServiceImpl.SERVICE_NAME);
                Address eventSource = event.getEventSource();
                MemberImpl member = clusterService.getMember(eventSource);
                ClientMessage eventMessage = ClientAddPartitionLostListenerCodec
                        .encodePartitionLostEvent(event.getPartitionId(), event.getLostBackupCount(), member.getUuid());
                sendClientMessage(null, eventMessage);
            }
        };

        if (parameters) {
            return newCompletedFuture(partitionService.addLocalPartitionLostListener(listener));
        }

        return partitionService.addPartitionLostListenerAsync(listener);
    }

    @Override
    protected Boolean decodeClientMessage(ClientMessage clientMessage) {
        return ClientAddPartitionLostListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientAddPartitionLostListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return IPartitionService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return PARTITION_LOST_EVENT_TOPIC;
    }

    @Override
    public String getMethodName() {
        return "addPartitionLostListener";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    protected String getUserCodeNamespace() {
        // Default Namespace
        return null;
    }
}
