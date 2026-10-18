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
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddWanReplicationConfigCodec;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.config.WanCustomPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AddWanReplicationConfigTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddWanReplicationConfigCodec.RequestParameters> {
    public AddWanReplicationConfigTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddWanReplicationConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddWanReplicationConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddWanReplicationConfigCodec.encodeResponse();
    }

    @Override
    public String getMethodName() {
        return "addWanReplicationConfig";
    }


    @Override
    protected IdentifiedDataSerializable getConfig() {
        WanReplicationConfigTransformer transformer = new WanReplicationConfigTransformer(serializationService);
        WanConsumerConfig wanConsumerConfig = transformer.toConfig(parameters.consumerConfig);

        List<WanCustomPublisherConfig> customPublisherConfigs =
                parameters.customPublisherConfigs.stream()
                                                 .filter(Objects::nonNull)
                                                 .map(transformer::toConfig)
                                                 .collect(Collectors.toList());
        List<WanBatchPublisherConfig> batchPublisherConfigs =
                parameters.batchPublisherConfigs.stream()
                                                 .filter(Objects::nonNull)
                                                 .map(transformer::toConfig)
                                                 .collect(Collectors.toList());

        WanReplicationConfig config = new WanReplicationConfig();
        config.setName(parameters.name);
        if (wanConsumerConfig != null) {
            config.setConsumerConfig(wanConsumerConfig);
        }
        config.setCustomPublisherConfigs(customPublisherConfigs);
        config.setBatchPublisherConfigs(batchPublisherConfigs);
        return config;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        DynamicConfigurationAwareConfig nodeConfig = (DynamicConfigurationAwareConfig) nodeEngine.getConfig();
        WanReplicationConfig wanReplicationConfig = (WanReplicationConfig) config;
        return DynamicConfigurationAwareConfig.checkStaticConfigDoesNotExist(
                nodeConfig.getStaticConfig().getWanReplicationConfigs(),
                wanReplicationConfig.getName(),
                wanReplicationConfig);
    }
}
