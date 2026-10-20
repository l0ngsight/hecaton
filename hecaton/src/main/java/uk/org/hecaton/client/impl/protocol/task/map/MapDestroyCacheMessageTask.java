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
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec#REQUEST_MESSAGE_TYPE}
 */
public class MapDestroyCacheMessageTask
        extends AbstractMultiTargetMessageTask<ContinuousQueryDestroyCacheCodec.RequestParameters>
        implements Supplier<Operation> {

    public MapDestroyCacheMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return this;
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        return !map.containsValue(Boolean.FALSE);
    }

    @Override
    public Collection<Member> getTargets() {
        return clientEngine.getClusterService().getMembers();
    }

    @Override
    protected ContinuousQueryDestroyCacheCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ContinuousQueryDestroyCacheCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ContinuousQueryDestroyCacheCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.mapName, ActionConstants.ACTION_DESTROY);
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.DESTROY_CACHE;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Operation get() {
        DestroyQueryCacheOperation operation = new DestroyQueryCacheOperation(parameters.mapName, parameters.cacheName);
        operation.setCallerUuid(endpoint.getUuid());
        return operation;
    }
}
