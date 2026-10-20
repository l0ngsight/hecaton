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
import uk.org.hecaton.client.impl.protocol.codec.MapAddIndexCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAllPartitionsMessageTask;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.AddIndexOperationFactory;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.Map;

public class MapAddIndexMessageTask
        extends AbstractAllPartitionsMessageTask<MapAddIndexCodec.RequestParameters> {

    public MapAddIndexMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new AddIndexOperationFactory(parameters.name, parameters.indexConfig);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        return null;
    }

    @Override
    protected MapAddIndexCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapAddIndexCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapAddIndexCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_INDEX);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_INDEX;
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.indexConfig};
    }

    @Override
    protected void beforeProcess() {
        if (nodeEngine.getConfig().getMapConfig(parameters.name).getInMemoryFormat() == InMemoryFormat.NATIVE
                && parameters.indexConfig.getType() == IndexType.BITMAP) {
            throw new IllegalArgumentException("BITMAP indexes are not supported by NATIVE storage");
        }
        if (nodeEngine.getClusterService().getClusterState() == ClusterState.PASSIVE) {
            throw new IllegalStateException("Cannot add index when cluster is in " + ClusterState.PASSIVE + " state!");
        }
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
