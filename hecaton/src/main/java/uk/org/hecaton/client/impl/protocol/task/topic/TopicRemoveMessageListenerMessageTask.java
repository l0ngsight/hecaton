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

package uk.org.hecaton.client.impl.protocol.task.topic;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.TopicRemoveMessageListenerCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractRemoveListenerMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.security.permission.TopicPermission;
import uk.org.hecaton.topic.impl.TopicService;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.Future;

public class TopicRemoveMessageListenerMessageTask
        extends AbstractRemoveListenerMessageTask<TopicRemoveMessageListenerCodec.RequestParameters> {

    public TopicRemoveMessageListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Future<Boolean> deRegisterListener() {
        TopicService service = getService(TopicService.SERVICE_NAME);
        return service.removeMessageListenerAsync(parameters.name, parameters.registrationId);
    }

    @Override
    protected UUID getRegistrationId() {
        return parameters.registrationId;
    }

    @Override
    protected TopicRemoveMessageListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TopicRemoveMessageListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TopicRemoveMessageListenerCodec.encodeResponse((Boolean) response);
    }


    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TopicPermission(getDistributedObjectName(), ActionConstants.ACTION_LISTEN);
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = TopicService.lookupNamespace(nodeEngine, getDistributedObjectName());
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.REMOVE_MESSAGE_LISTENER;
    }

}
