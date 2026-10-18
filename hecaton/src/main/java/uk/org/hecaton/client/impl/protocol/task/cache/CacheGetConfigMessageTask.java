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

import uk.org.hecaton.cache.impl.operation.CacheGetConfigOperation;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheGetConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.holder.CacheConfigHolder;
import uk.org.hecaton.client.impl.protocol.task.AbstractTargetMessageTask;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.UUID;

import static uk.org.hecaton.cache.impl.ICacheService.SERVICE_NAME;

/**
 * This client request specifically calls {@link CacheGetConfigOperation} on the server side.
 *
 * @see CacheGetConfigOperation
 */
public class CacheGetConfigMessageTask
        extends AbstractTargetMessageTask<CacheGetConfigCodec.RequestParameters> {

    public CacheGetConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected UUID getTargetUuid() {
        return nodeEngine.getClusterService().getLocalMember().getUuid();
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheGetConfigOperation(parameters.name, parameters.simpleName);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.GET_CONFIG;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    protected CacheGetConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheGetConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        CacheConfig cacheConfig = (CacheConfig) response;

        return CacheGetConfigCodec.encodeResponse(CacheConfigHolder.of(cacheConfig, serializationService));
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
