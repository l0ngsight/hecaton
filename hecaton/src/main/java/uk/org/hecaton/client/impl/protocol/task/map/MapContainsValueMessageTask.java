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
import uk.org.hecaton.client.impl.protocol.codec.MapContainsValueCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

import static uk.org.hecaton.map.impl.LocalMapStatsUtil.incrementOtherOperationsCount;

public class MapContainsValueMessageTask
        extends AbstractMapAllPartitionsMessageTask<MapContainsValueCodec.RequestParameters> {

    public MapContainsValueMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        MapOperationProvider operationProvider = getOperationProvider(parameters.name);
        return operationProvider.createContainsValueOperationFactory(parameters.name, parameters.value);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        boolean result = false;
        for (Object contains : map.values()) {
            if (Boolean.TRUE.equals(contains)) {
                result = true;
                break;
            }
        }
        incrementOtherOperationsCount(getService(MapService.SERVICE_NAME), parameters.name);
        return result;
    }

    @Override
    protected MapContainsValueCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapContainsValueCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapContainsValueCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CONTAINS_VALUE;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.value};
    }


}
