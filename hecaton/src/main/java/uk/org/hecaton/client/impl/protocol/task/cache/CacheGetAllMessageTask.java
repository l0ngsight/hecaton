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

package uk.org.hecaton.client.impl.protocol.task.cache;

import uk.org.hecaton.cache.impl.CacheOperationProvider;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.operation.CacheGetAllOperationFactory;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheGetAllCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import java.security.Permission;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This client request  specifically calls {@link CacheGetAllOperationFactory} on the server side.
 *
 * @see CacheGetAllOperationFactory
 */
public class CacheGetAllMessageTask
        extends AbstractCacheAllPartitionsTask<CacheGetAllCodec.RequestParameters> {

    public CacheGetAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CacheGetAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheGetAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheGetAllCodec.encodeResponse((List<Map.Entry<Data, Data>>) response);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        CacheService service = getService(getServiceName());
        ExpiryPolicy expiryPolicy = (ExpiryPolicy) service.toObject(parameters.expiryPolicy);
        Set<Data> keys = new HashSet<>(parameters.keys);
        return operationProvider.createGetAllOperationFactory(keys, expiryPolicy);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        List<Map.Entry<Data, Data>> reducedMap = new ArrayList<>(map.size());
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            MapEntries mapEntries = (MapEntries) nodeEngine.toObject(entry.getValue());
            mapEntries.putAllToList(reducedMap);
        }
        return reducedMap;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.GET_ALL;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.keys};
    }

}
