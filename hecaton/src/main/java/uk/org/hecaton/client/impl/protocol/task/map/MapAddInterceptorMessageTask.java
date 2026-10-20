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

import uk.org.hecaton.map.impl.operation.AddInterceptorOperationSupplier;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MapAddInterceptorCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.MapInterceptor;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

public class MapAddInterceptorMessageTask
        extends AbstractMultiTargetMessageTask<MapAddInterceptorCodec.RequestParameters> {

    private transient String id;

    public MapAddInterceptorMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        final MapService mapService = getService(MapService.SERVICE_NAME);
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final MapInterceptor mapInterceptor =
                NamespaceUtil.callWithNamespace(nodeEngine, MapService.lookupNamespace(nodeEngine, parameters.name),
                        () -> serializationService.toObject(parameters.interceptor));
        id = mapServiceContext.generateInterceptorId(parameters.name, mapInterceptor);
        return new AddInterceptorOperationSupplier(parameters.name, id, mapInterceptor);
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        for (Object result : map.values()) {
            if (result instanceof Throwable throwable) {
                throw throwable;
            }
        }
        return id;
    }


    @Override
    public Collection<Member> getTargets() {
        return nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected MapAddInterceptorCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddInterceptorCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddInterceptorCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_INTERCEPT);
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = MapService.lookupNamespace(nodeEngine, getDistributedObjectName());
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_INTERCEPTOR;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.interceptor};
    }
}
