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

import uk.org.hecaton.cache.impl.CacheContext;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.client.impl.ClientEndpoint;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAddListenerMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidation;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.security.SecurityInterceptorConstants;

import java.security.Permission;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class CacheAddNearCacheInvalidationListenerTask
        extends AbstractAddListenerMessageTask<CacheAddNearCacheInvalidationListenerCodec.RequestParameters> {

    public CacheAddNearCacheInvalidationListenerTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        CacheService cacheService = getService(ICacheService.SERVICE_NAME);
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(parameters.name);
        NearCacheInvalidationListener listener
                = new NearCacheInvalidationListener(endpoint, cacheContext,
                nodeEngine.getLocalMember().getUuid(), clientMessage.getCorrelationId());

        return parameters.localOnly ? newCompletedFuture(
                cacheService.registerLocalListener(parameters.name, listener)) : (CompletableFuture<UUID>) cacheService
                .registerListenerAsync(parameters.name, listener);
    }

    private final class NearCacheInvalidationListener extends AbstractCacheClientNearCacheInvalidationListener {

        NearCacheInvalidationListener(ClientEndpoint endpoint, CacheContext cacheContext,
                                      UUID localMemberUuid, long correlationId) {
            super(endpoint, cacheContext, localMemberUuid, correlationId);
        }

        @Override
        protected ClientMessage encodeBatchInvalidation(String name, List<Data> keys, List<UUID> sourceUuids,
                                                        List<UUID> partitionUuids, List<Long> sequences) {
            return CacheAddNearCacheInvalidationListenerCodec.encodeCacheBatchInvalidationEvent(name, keys,
                    sourceUuids, partitionUuids, sequences);
        }

        @Override
        protected ClientMessage encodeSingleInvalidation(String name, Data key, UUID sourceUuid,
                                                         UUID partitionUuid, long sequence) {
            return CacheAddNearCacheInvalidationListenerCodec.encodeCacheInvalidationEvent(name, key,
                    sourceUuid, partitionUuid, sequence);
        }

        @Override
        protected void sendMessageWithOrderKey(ClientMessage clientMessage, Object orderKey) {
            sendClientMessage(orderKey, clientMessage);
        }

        @Override
        protected boolean canSendInvalidation(Invalidation invalidation) {
            return true;
        }
    }

    @Override
    protected CacheAddNearCacheInvalidationListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddNearCacheInvalidationListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddNearCacheInvalidationListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_NEAR_CACHE_INVALIDATION_LISTENER;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    protected String getUserCodeNamespace() {
        return CacheService.lookupNamespace(nodeEngine, parameters.name);
    }
}
