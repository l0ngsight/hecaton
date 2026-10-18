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
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddFlakeIdGeneratorConfigCodec;
import uk.org.hecaton.config.FlakeIdGeneratorConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.SecurityInterceptorConstants;

public class AddFlakeIdGeneratorConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddFlakeIdGeneratorConfigCodec.RequestParameters> {

    public AddFlakeIdGeneratorConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddFlakeIdGeneratorConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddFlakeIdGeneratorConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddFlakeIdGeneratorConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        FlakeIdGeneratorConfig config = new FlakeIdGeneratorConfig(parameters.name);
        config.setPrefetchCount(parameters.prefetchCount);
        config.setPrefetchValidityMillis(parameters.prefetchValidity);
        config.setNodeIdOffset(parameters.nodeIdOffset);
        config.setEpochStart(parameters.epochStart);
        config.setBitsSequence(parameters.bitsSequence);
        config.setBitsNodeId(parameters.bitsNodeId);
        config.setAllowedFutureMillis(parameters.allowedFutureMillis);
        config.setStatisticsEnabled(parameters.statisticsEnabled);
        return config;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_FLAKE_ID_GENERATOR_CONFIG;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        FlakeIdGeneratorConfig flakeIdGeneratorConfig = (FlakeIdGeneratorConfig) config;
        return nodeConfig.checkStaticConfigDoesNotExist(nodeConfig.getStaticConfig().getFlakeIdGeneratorConfigs(),
                flakeIdGeneratorConfig.getName(), flakeIdGeneratorConfig);
    }
}
