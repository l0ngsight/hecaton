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

package uk.org.hecaton.client.impl.protocol.task.dynamicconfig;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddReliableTopicConfigCodec;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.ReliableTopicConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.topic.TopicOverloadPolicy;

import java.security.Permission;
import java.util.List;
import java.util.concurrent.Executor;

public class AddReliableTopicConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddReliableTopicConfigCodec.RequestParameters> {

    public AddReliableTopicConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddReliableTopicConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddReliableTopicConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddReliableTopicConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        ReliableTopicConfig config = new ReliableTopicConfig(parameters.name);
        config.setStatisticsEnabled(parameters.statisticsEnabled);
        config.setReadBatchSize(parameters.readBatchSize);
        config.setTopicOverloadPolicy(TopicOverloadPolicy.valueOf(parameters.topicOverloadPolicy));
        Executor executor = serializationService.toObject(parameters.executor);
        config.setExecutor(executor);
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            config.setMessageListenerConfigs(
                    (List<ListenerConfig>) adaptListenerConfigs(parameters.listenerConfigs, parameters.userCodeNamespace));
        }
        if (parameters.isUserCodeNamespaceExists) {
            config.setUserCodeNamespace(parameters.userCodeNamespace);
        }
        return config;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_RELIABLE_TOPIC_CONFIG;
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        return parameters.userCodeNamespace != null
                ? new UserCodeNamespacePermission(parameters.userCodeNamespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        ReliableTopicConfig reliableTopicConfig = (ReliableTopicConfig) config;
        return DynamicConfigurationAwareConfig.checkStaticConfigDoesNotExist(
                nodeConfig.getStaticConfig().getReliableTopicConfigs(), reliableTopicConfig.getName(), reliableTopicConfig);
    }
}
