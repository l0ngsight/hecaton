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
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddExecutorConfigCodec;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;

import java.security.Permission;

public class AddExecutorConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddExecutorConfigCodec.RequestParameters> {

    public AddExecutorConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddExecutorConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddExecutorConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddExecutorConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        ExecutorConfig config = new ExecutorConfig(parameters.name, parameters.poolSize);
        config.setQueueCapacity(parameters.queueCapacity);
        config.setStatisticsEnabled(parameters.statisticsEnabled);
        if (parameters.isUserCodeNamespaceExists) {
            config.setUserCodeNamespace(parameters.userCodeNamespace);
        }
        return config;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_EXECUTOR_CONFIG;
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        return parameters.userCodeNamespace != null
                ? new UserCodeNamespacePermission(parameters.userCodeNamespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        ExecutorConfig executorConfig = (ExecutorConfig) config;
        return DynamicConfigurationAwareConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getExecutorConfigs(),
                executorConfig.getName(), executorConfig);
    }
}
