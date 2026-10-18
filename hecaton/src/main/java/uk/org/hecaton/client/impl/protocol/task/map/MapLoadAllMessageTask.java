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
import uk.org.hecaton.client.impl.protocol.codec.MapLoadAllCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractCallableMessageTask;
import uk.org.hecaton.client.impl.protocol.task.BlockingMessageTask;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;

import java.security.Permission;
import java.util.UUID;

public class MapLoadAllMessageTask
        extends AbstractCallableMessageTask<MapLoadAllCodec.RequestParameters> implements BlockingMessageTask {

    public MapLoadAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        final MapService mapService = getService(MapService.SERVICE_NAME);
        UUID source = endpoint.getUuid();
        final DistributedObject distributedObject
                = mapService.getMapServiceContext().getNodeEngine().getProxyService()
                .getDistributedObject(MapService.SERVICE_NAME, parameters.name, source);
        final MapProxyImpl mapProxy = (MapProxyImpl) distributedObject;
        mapProxy.loadAll(parameters.replaceExistingValues);
        return null;
    }

    @Override
    protected MapLoadAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapLoadAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapLoadAllCodec.encodeResponse();
    }


    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }


    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.LOAD_ALL;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.replaceExistingValues};
    }
}
