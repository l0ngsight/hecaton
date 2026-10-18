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

package uk.org.hecaton.client.impl.protocol.task.map;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MapClearCodec;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.impl.LocalMapStatsProvider;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.event.MapEventPublisher;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

import static uk.org.hecaton.core.EntryEventType.CLEAR_ALL;

public class MapClearMessageTask
        extends AbstractMapAllPartitionsMessageTask<String> {

    public MapClearMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        MapOperationProvider operationProvider = getOperationProvider(parameters);
        return operationProvider.createClearOperationFactory(parameters);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int clearedTotal = 0;
        for (Object affectedEntries : map.values()) {
            clearedTotal += (Integer) affectedEntries;
        }

        MapService service = getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();

        if (clearedTotal > 0) {
            Address thisAddress = nodeEngine.getThisAddress();
            MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
            mapEventPublisher.publishMapEvent(thisAddress, parameters, CLEAR_ALL, clearedTotal);
        }

        final MapService mapService = getService(MapService.SERVICE_NAME);
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(parameters);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
            localMapStatsProvider.getLocalMapStatsImpl(parameters).incrementOtherOperations();
        }
        return null;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MapClearCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapClearCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CLEAR;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
