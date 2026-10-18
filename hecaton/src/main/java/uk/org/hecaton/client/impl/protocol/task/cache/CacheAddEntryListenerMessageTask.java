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
import uk.org.hecaton.cache.impl.CacheEventData;
import uk.org.hecaton.cache.impl.CacheEventListener;
import uk.org.hecaton.cache.impl.CacheEventSet;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.client.impl.ClientEndpoint;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAddListenerMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.internal.services.ListenerWrapperEventFilter;
import uk.org.hecaton.internal.services.NotifiableEventListener;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;

import java.security.Permission;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

/**
 * Client request which registers an event listener on behalf of the client and delegates the received events
 * back to client.
 *
 * @see CacheService#registerListener(String, CacheEventListener)
 */
public class CacheAddEntryListenerMessageTask
        extends AbstractAddListenerMessageTask<CacheAddEntryListenerCodec.RequestParameters> {

    public CacheAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        final CacheService service = getService(ICacheService.SERVICE_NAME);
        CacheEntryListener cacheEntryListener = new CacheEntryListener(endpoint, this);

        if (parameters.localOnly) {
            return newCompletedFuture(service.registerLocalListener(parameters.name, cacheEntryListener, cacheEntryListener));
        }

        return service.registerListenerAsync(parameters.name, cacheEntryListener, cacheEntryListener);
    }

    @Override
    protected void addDestroyAction(UUID registrationId) {
        final CacheService service = getService(ICacheService.SERVICE_NAME);
        endpoint.addDestroyAction(registrationId, () -> service.deregisterListener(parameters.name, registrationId));
    }

    private static final class CacheEntryListener
            implements CacheEventListener,
            NotifiableEventListener<CacheService>,
            ListenerWrapperEventFilter {

        private final ClientEndpoint endpoint;
        private final CacheAddEntryListenerMessageTask cacheAddEntryListenerMessageTask;

        private CacheEntryListener(ClientEndpoint endpoint,
                                   CacheAddEntryListenerMessageTask cacheAddEntryListenerMessageTask) {
            this.endpoint = endpoint;
            this.cacheAddEntryListenerMessageTask = cacheAddEntryListenerMessageTask;
        }

        private Data getPartitionKey(Object eventObject) {
            Data partitionKey = null;
            if (eventObject instanceof CacheEventSet cacheEventSet) {
                Set<CacheEventData> events = cacheEventSet.getEvents();
                if (events.size() > 1) {
                    partitionKey = new HeapData();
                } else if (events.size() == 1) {
                    partitionKey = events.iterator().next().getDataKey();
                }
            } else if (eventObject instanceof CacheEventData cacheEventData) {
                partitionKey = cacheEventData.getDataKey();
            }
            return partitionKey;
        }

        @Override
        public void handleEvent(Object eventObject) {
            if (!endpoint.isAlive()) {
                return;
            }
            if (eventObject instanceof CacheEventSet ces) {
                Data partitionKey = getPartitionKey(eventObject);
                ClientMessage clientMessage =
                        CacheAddEntryListenerCodec.
                                encodeCacheEvent(ces.getEventType().getType(), ces.getEvents(), ces.getCompletionId());
                cacheAddEntryListenerMessageTask.sendClientMessage(partitionKey, clientMessage);
            }
        }

        @Override
        public void onRegister(CacheService service, String serviceName,
                               String topic, EventRegistration registration) {
            CacheContext cacheContext = service.getOrCreateCacheContext(topic);
            cacheContext.increaseCacheEntryListenerCount();
        }

        @Override
        public void onDeregister(CacheService service, String serviceName,
                                 String topic, EventRegistration registration) {
            CacheContext cacheContext = service.getOrCreateCacheContext(topic);
            cacheContext.decreaseCacheEntryListenerCount();
        }

        @Override
        public Object getListener() {
            return this;
        }

        @Override
        public boolean eval(Object event) {
            return true;
        }

    }

    @Override
    protected CacheAddEntryListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddEntryListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddEntryListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getServiceName() {
        return SecurityInterceptorConstants.ICACHE_SERVICE;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_ENTRY_LISTENER;
    }

    @Override
    protected String getUserCodeNamespace() {
        return CacheService.lookupNamespace(nodeEngine, parameters.name);
    }
}
