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

package uk.org.hecaton.client.impl.protocol.task.replicatedmap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapGetCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.monitor.impl.LocalReplicatedMapStatsImpl;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.util.Timer;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.operation.GetOperation;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ReplicatedMapPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

public class ReplicatedMapGetMessageTask
        extends AbstractPartitionMessageTask<ReplicatedMapGetCodec.RequestParameters> {

    private volatile long startTimeNanos;

    public ReplicatedMapGetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetOperation(parameters.name, parameters.key);
    }

    @Override
    protected ReplicatedMapGetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapGetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected void beforeProcess() {
        startTimeNanos = Timer.nanos();
    }

    @Override
    protected Object processResponseBeforeSending(Object response) {
        ReplicatedMapService replicatedMapService = getService(ReplicatedMapService.SERVICE_NAME);
        if (replicatedMapService.getReplicatedMapConfig(parameters.name).isStatisticsEnabled()) {
            LocalReplicatedMapStatsImpl stats = replicatedMapService.getLocalReplicatedMapStatsImpl(parameters.name);
            stats.incrementGetsNanos(Timer.nanosElapsed(startTimeNanos));
        }
        return response;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ReplicatedMapGetCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.GET;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key};
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
