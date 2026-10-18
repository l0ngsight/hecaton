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

import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractRemoveListenerMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.security.SecurityInterceptorConstants;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.Future;

public class CacheRemovePartitionLostListenerMessageTask
        extends AbstractRemoveListenerMessageTask<CacheRemovePartitionLostListenerCodec.RequestParameters> {


    public CacheRemovePartitionLostListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Future<Boolean> deRegisterListener() {
        ICacheService service = getService(CacheService.SERVICE_NAME);
        return service.getNodeEngine().getEventService()
                      .deregisterListenerAsync(ICacheService.SERVICE_NAME, parameters.name, parameters.registrationId);
    }

    @Override
    protected UUID getRegistrationId() {
        return parameters.registrationId;
    }

    @Override
    protected CacheRemovePartitionLostListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheRemovePartitionLostListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheRemovePartitionLostListenerCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.REMOVE_PARTITION_LOST_LISTENER;
    }

}
