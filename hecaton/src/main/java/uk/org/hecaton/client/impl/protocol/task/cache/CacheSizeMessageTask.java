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
import uk.org.hecaton.cache.impl.operation.CacheSizeOperationFactory;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheSizeCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

import static uk.org.hecaton.internal.util.MapUtil.toIntSize;

/**
 * This client request specifically calls {@link CacheSizeOperationFactory} on the server side.
 *
 * @see CacheSizeOperationFactory
 */
public class CacheSizeMessageTask
        extends AbstractCacheAllPartitionsTask<String> {

    public CacheSizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters);
        return operationProvider.createSizeOperationFactory();
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        long total = 0;
        CacheService service = getService(getServiceName());
        for (Object result : map.values()) {
            Integer size = (Integer) service.toObject(result);
            total += size;
        }
        return toIntSize(total);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return CacheSizeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheSizeCodec.encodeResponse((Integer) response);
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SIZE;
    }
}
