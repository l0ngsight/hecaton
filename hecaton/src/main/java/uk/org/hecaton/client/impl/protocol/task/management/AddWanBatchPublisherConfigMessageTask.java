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

package uk.org.hecaton.client.impl.protocol.task.management;

import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec.RequestParameters;
import uk.org.hecaton.client.impl.protocol.task.AbstractCallableMessageTask;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConsistencyCheckStrategy;
import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanQueueFullBehavior;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanSyncConfig;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.security.permission.ManagementPermission;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.wan.impl.AddWanConfigResult;
import uk.org.hecaton.wan.impl.WanReplicationService;

import java.security.Permission;

import static uk.org.hecaton.config.WanBatchPublisherConfig.DEFAULT_ACKNOWLEDGE_TYPE;
import static uk.org.hecaton.config.WanBatchPublisherConfig.DEFAULT_QUEUE_FULL_BEHAVIOUR;

public class AddWanBatchPublisherConfigMessageTask extends AbstractCallableMessageTask<RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("wan.addBatchPublisherConfig");

    public AddWanBatchPublisherConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    public AddWanBatchPublisherConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
            InternalSerializationService serializationService, ClientEngine clientEngine, Connection connection,
            NodeExtension nodeExtension, BuildInfo buildInfo, Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    @Override
    protected Object call() throws Exception {
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName(parameters.name);

        WanBatchPublisherConfig publisherConfig = new WanBatchPublisherConfig();
        publisherConfig.setPublisherId(parameters.publisherId);
        publisherConfig.setClusterName(parameters.targetCluster);
        publisherConfig.setTargetEndpoints(parameters.endpoints);
        publisherConfig.setQueueCapacity(parameters.queueCapacity);
        publisherConfig.setBatchSize(parameters.batchSize);
        publisherConfig.setBatchMaxDelayMillis(parameters.batchMaxDelayMillis);
        publisherConfig.setResponseTimeoutMillis(parameters.responseTimeoutMillis);

        WanAcknowledgeType ackType = WanAcknowledgeType.getById(parameters.ackType);
        publisherConfig.setAcknowledgeType(ackType != null ? ackType : DEFAULT_ACKNOWLEDGE_TYPE);

        WanQueueFullBehavior queueFullBehavior = WanQueueFullBehavior.getByType(parameters.queueFullBehavior);
        publisherConfig.setQueueFullBehavior(
                queueFullBehavior != null ? queueFullBehavior : DEFAULT_QUEUE_FULL_BEHAVIOUR);
        if (parameters.isConsistencyCheckStrategyExists) {
            WanSyncConfig syncConfig = new WanSyncConfig();
            syncConfig.setConsistencyCheckStrategy(ConsistencyCheckStrategy.getById(parameters.consistencyCheckStrategy));
            publisherConfig.setSyncConfig(syncConfig);
        }
        wanConfig.addBatchReplicationPublisherConfig(publisherConfig);

        return nodeEngine.getWanReplicationService().addWanReplicationConfig(wanConfig);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCAddWanBatchPublisherConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        AddWanConfigResult result = (AddWanConfigResult) response;
        return MCAddWanBatchPublisherConfigCodec.encodeResponse(
                result.getAddedPublisherIds(), result.getIgnoredPublisherIds());
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "addWanBatchPublisherConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{
                parameters.name, parameters.targetCluster, parameters.publisherId, parameters.endpoints,
                parameters.queueCapacity, parameters.batchSize, parameters.batchMaxDelayMillis,
                parameters.responseTimeoutMillis, parameters.ackType, parameters.queueFullBehavior};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
