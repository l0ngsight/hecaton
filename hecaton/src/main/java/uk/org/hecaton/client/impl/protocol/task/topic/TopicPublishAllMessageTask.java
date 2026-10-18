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
import uk.org.hecaton.client.impl.protocol.codec.TopicPublishAllCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.TopicPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.topic.impl.PublishAllOperation;
import uk.org.hecaton.topic.impl.TopicService;

import java.security.Permission;
import java.util.List;

public class TopicPublishAllMessageTask
        extends AbstractPartitionMessageTask<TopicPublishAllCodec.RequestParameters> {

    public TopicPublishAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new PublishAllOperation(parameters.name, items());
    }

    @Override
    protected TopicPublishAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TopicPublishAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TopicPublishAllCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TopicPermission(parameters.name, ActionConstants.ACTION_PUBLISH);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.PUBLISH_ALL;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.messages};
    }

    private Data[] items() {
        List<Data> messageList = parameters.messages;
        Data[] array = new Data[messageList.size()];
        return messageList.toArray(array);
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
