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
import uk.org.hecaton.client.impl.protocol.codec.MapFlushCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractCallableMessageTask;
import uk.org.hecaton.client.impl.protocol.task.BlockingMessageTask;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.proxyservice.ProxyService;

import java.security.Permission;
import java.util.UUID;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;

public class MapFlushMessageTask
        extends AbstractCallableMessageTask<String> implements BlockingMessageTask {

    public MapFlushMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        MapService mapService = getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        ProxyService proxyService = nodeEngine.getProxyService();
        UUID source = endpoint.getUuid();
        DistributedObject distributedObject = proxyService.getDistributedObject(SERVICE_NAME, parameters, source);

        MapProxyImpl mapProxy = (MapProxyImpl) distributedObject;
        mapProxy.flush();

        return null;
    }


    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MapFlushCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapFlushCodec.encodeResponse();
    }


    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.FLUSH;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

}
