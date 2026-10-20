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

package uk.org.hecaton.client.impl.protocol.task.management;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCWanSyncMapCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCWanSyncMapCodec.RequestParameters;
import uk.org.hecaton.client.impl.protocol.task.AbstractCallableMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ManagementPermission;
import uk.org.hecaton.wan.impl.WanReplicationService;
import uk.org.hecaton.wan.impl.WanSyncType;

import java.security.Permission;
import java.util.UUID;

public class WanSyncMapMessageTask extends AbstractCallableMessageTask<RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("wan.syncMap");

    public WanSyncMapMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        WanSyncType wanSyncType = WanSyncType.getByType(parameters.wanSyncType);
        if (wanSyncType == null) {
            throw new IllegalArgumentException(
                    String.format("Invalid wanSyncType: %s", parameters.wanSyncType));
        }
        switch (wanSyncType) {
            case ALL_MAPS:
                return syncAllMaps();
            case SINGLE_MAP:
                return syncSingleMap();
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported wanSyncType: %s", parameters.wanSyncType));
        }
    }

    private UUID syncSingleMap() {
        return nodeEngine.getWanReplicationService()
                .syncMap(parameters.wanReplicationName, parameters.wanPublisherId, parameters.mapName);
    }

    private UUID syncAllMaps() {
        return nodeEngine.getWanReplicationService()
                .syncAllMaps(parameters.wanReplicationName, parameters.wanPublisherId);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCWanSyncMapCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCWanSyncMapCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "wanSyncMap";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{
                parameters.wanReplicationName,
                parameters.wanPublisherId,
                parameters.wanSyncType,
                parameters.mapName
        };
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
