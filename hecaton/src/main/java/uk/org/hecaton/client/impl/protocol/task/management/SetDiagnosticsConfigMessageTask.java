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

import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCSetDiagnosticsConfigCodec;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AbstractAddConfigMessageTask;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.internal.diagnostics.DiagnosticsConfig;
import uk.org.hecaton.internal.diagnostics.DiagnosticsOutputType;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.permission.ManagementPermission;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.security.Permission;

public class SetDiagnosticsConfigMessageTask
        extends AbstractAddConfigMessageTask<MCSetDiagnosticsConfigCodec.RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("cluster.setDiagnosticsConfig");

    public SetDiagnosticsConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    public SetDiagnosticsConfigMessageTask(ClientMessage clientMessage, ILogger logger, NodeEngine nodeEngine,
                                           InternalSerializationService serializationService, ClientEngine clientEngine,
                                           Connection connection, NodeExtension nodeExtension, BuildInfo buildInfo,
                                           Config config, ClusterServiceImpl clusterService) {
        super(clientMessage, logger, nodeEngine, serializationService, clientEngine, connection, nodeExtension, buildInfo,
                config, clusterService);
    }

    @Override
    protected MCSetDiagnosticsConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCSetDiagnosticsConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCSetDiagnosticsConfigCodec.encodeResponse();
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(parameters.enabled)
                .setIncludeEpochTime(parameters.includeEpochTime)
                .setOutputType(DiagnosticsOutputType.valueOf(parameters.outputType))
                .setMaxRolledFileCount(parameters.maxRolledFileCount)
                .setMaxRolledFileSizeInMB(parameters.maxRolledFileSizeInMB)
                .setLogDirectory(parameters.logDirectory)
                .setFileNamePrefix(parameters.fileNamePrefix)
                .setAutoOffDurationInMinutes(parameters.autoOffDurationInMinutes);

        if (parameters.properties != null && !parameters.properties.isEmpty()) {
            diagnosticsConfig.getPluginProperties().putAll(parameters.properties);
        }

        return diagnosticsConfig;
    }


    @Override
    public String getMethodName() {
        return "setDiagnosticsConfig";
    }

    @Override
    protected boolean checkStaticConfigDoesNotExist(IdentifiedDataSerializable config) {
        return true;
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
