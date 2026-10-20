/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal.client;

import uk.org.hecaton.client.impl.ClientEndpoint;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPSubsystemAddGroupAvailabilityListenerCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAsyncMessageTask;
import uk.org.hecaton.cp.event.CPGroupAvailabilityEvent;
import uk.org.hecaton.cp.event.CPGroupAvailabilityListener;
import uk.org.hecaton.cp.event.impl.CPGroupAvailabilityEventImpl;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.client.impl.protocol.codec.CPSubsystemAddGroupAvailabilityListenerCodec.encodeGroupAvailabilityEventEvent;
import static uk.org.hecaton.cp.internal.RaftService.EVENT_TOPIC_AVAILABILITY;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;

public class AddCPGroupAvailabilityListenerMessageTask extends AbstractAsyncMessageTask<Boolean, UUID> {

    private static final String TOPIC = EVENT_TOPIC_AVAILABILITY;

    public AddCPGroupAvailabilityListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        EventService eventService = clientEngine.getEventService();
        CPGroupAvailabilityListener listener = new ClientCPGroupAvailabilityListener(endpoint);

        return eventService.registerListenerAsync(getServiceName(), TOPIC, listener)
                .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
    }

    private class ClientCPGroupAvailabilityListener implements CPGroupAvailabilityListener {
        private final ClientEndpoint endpoint;

        ClientCPGroupAvailabilityListener(ClientEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void availabilityDecreased(CPGroupAvailabilityEvent event) {
            if (!endpoint.isAlive()) {
                return;
            }
            boolean isShutdown = ((CPGroupAvailabilityEventImpl) event).isShutdown();
            ClientMessage message = encodeGroupAvailabilityEventEvent((RaftGroupId) event.getGroupId(),
                    event.getGroupMembers(), event.getUnavailableMembers(), isShutdown);
            sendClientMessage(message);
        }

        @Override
        public void majorityLost(CPGroupAvailabilityEvent event) {
            if (!endpoint.isAlive()) {
                return;
            }
            boolean isShutdown = ((CPGroupAvailabilityEventImpl) event).isShutdown();
            ClientMessage message = encodeGroupAvailabilityEventEvent((RaftGroupId) event.getGroupId(),
                    event.getGroupMembers(), event.getUnavailableMembers(), isShutdown);
            sendClientMessage(message);
        }
    }

    @Override
    protected Object processResponseBeforeSending(UUID registrationId) {
        endpoint.addListenerDestroyAction(getServiceName(), TOPIC, registrationId);
        return registrationId;
    }

    @Override
    protected Boolean decodeClientMessage(ClientMessage clientMessage) {
        return CPSubsystemAddGroupAvailabilityListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSubsystemAddGroupAvailabilityListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "addCPGroupAvailabilityListener";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    public Permission getRequiredPermission() {
        return null;
    }
}
