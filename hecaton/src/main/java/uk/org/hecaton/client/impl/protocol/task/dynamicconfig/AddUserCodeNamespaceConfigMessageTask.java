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

package uk.org.hecaton.client.impl.protocol.task.dynamicconfig;

import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddUserCodeNamespaceConfigCodec;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigAccessor;
import uk.org.hecaton.config.UserCodeNamespaceConfig;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.security.Permission;

public class AddUserCodeNamespaceConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddUserCodeNamespaceConfigCodec.RequestParameters> {

    public AddUserCodeNamespaceConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected AddUserCodeNamespaceConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
            InternalSerializationService serializationService, ClientEngine clientEngine, Connection connection,
            NodeExtension nodeExtension, BuildInfo buildInfo, Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    @Override
    protected DynamicConfigAddUserCodeNamespaceConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddUserCodeNamespaceConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddUserCodeNamespaceConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        UserCodeNamespaceConfig config = new UserCodeNamespaceConfig(parameters.name);
        parameters.resources.forEach(holder -> ConfigAccessor.add(config, holder));
        return config;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_NAMESPACE_CONFIG;
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        return parameters.name != null ? new UserCodeNamespacePermission(parameters.name, ActionConstants.ACTION_CREATE) : null;
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        // Support add-or-replace semantics
        return true;
    }
}
