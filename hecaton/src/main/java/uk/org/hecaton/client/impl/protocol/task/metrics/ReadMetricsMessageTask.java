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

package uk.org.hecaton.client.impl.protocol.task.metrics;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCReadMetricsCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractInvocationMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.metrics.impl.MetricsService;
import uk.org.hecaton.internal.metrics.managementcenter.ConcurrentArrayRingbuffer;
import uk.org.hecaton.internal.metrics.managementcenter.ReadMetricsOperation;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ManagementPermission;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.List;
import java.util.Map;

public class ReadMetricsMessageTask extends AbstractInvocationMessageTask<MCReadMetricsCodec.RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("metrics.read");

    public ReadMetricsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(),
                op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        // readMetrics requests are sent to member identified by address, but we want it by member UUID.
        // After a member restart, the address remains, but UUID changes. If the local member has different
        // UUID from the intended one, fail.
        if (!parameters.uuid.equals(nodeEngine.getLocalMember().getUuid())) {
            // do not throw RetryableException here
            throw new IllegalArgumentException(
                    "Requested metrics for member " + parameters.uuid
                            + ", but local member is " + nodeEngine.getLocalMember().getUuid()
            );
        }
        return new ReadMetricsOperation(parameters.fromSequence);
    }

    @Override
    protected MCReadMetricsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCReadMetricsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        ConcurrentArrayRingbuffer.RingbufferSlice<Map.Entry<Long, byte[]>> ringbufferSlice
                = (ConcurrentArrayRingbuffer.RingbufferSlice<Map.Entry<Long, byte[]>>) response;

        List<Map.Entry<Long, byte[]>> elements = ringbufferSlice.elements();
        long nextSequence = ringbufferSlice.nextSequence();
        return MCReadMetricsCodec.encodeResponse(elements, nextSequence);
    }

    @Override
    public String getServiceName() {
        return MetricsService.SERVICE_NAME;
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
        return SecurityInterceptorConstants.READ_METRICS;
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.uuid, parameters.fromSequence};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }

}

