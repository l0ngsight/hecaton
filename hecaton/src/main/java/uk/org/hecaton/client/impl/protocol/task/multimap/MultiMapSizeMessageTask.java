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

package uk.org.hecaton.client.impl.protocol.task.multimap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapSizeCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.multimap.impl.operations.MultiMapOperationFactory;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MultiMapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

import static uk.org.hecaton.internal.util.MapUtil.toIntSize;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.MultiMapSizeCodec#REQUEST_MESSAGE_TYPE}
 */
public class MultiMapSizeMessageTask
        extends AbstractAllPartitionsMessageTask<String> {

    public MultiMapSizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(parameters, MultiMapOperationFactory.OperationFactoryType.SIZE);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        long total = 0;
        for (Object obj : map.values()) {
            total += (Integer) obj;
        }
        if (getSampleContainer().getConfig().isStatisticsEnabled()) {
            ((MultiMapService) getService(MultiMapService.SERVICE_NAME)).getLocalMultiMapStatsImpl(parameters)
                    .incrementOtherOperations();
        }
        return toIntSize(total);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapSizeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapSizeCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SIZE;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    private MultiMapContainer getSampleContainer() {
        MultiMapService service = getService(MultiMapService.SERVICE_NAME);
        return service.getOrCreateCollectionContainer(0, parameters);
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
