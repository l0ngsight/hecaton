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

package uk.org.hecaton.internal.server.tcp;

import uk.org.hecaton.client.impl.protocol.util.ClientMessageDecoder;
import uk.org.hecaton.client.impl.protocol.util.ClientMessageEncoder;
import uk.org.hecaton.config.EndpointConfig;
import uk.org.hecaton.internal.loadprotection.ClientAdmission;
import uk.org.hecaton.internal.loadprotection.MaintenanceLane;
import uk.org.hecaton.internal.networking.Channel;
import uk.org.hecaton.internal.server.ServerContext;
import uk.org.hecaton.internal.server.ServerConnection;

import static uk.org.hecaton.instance.ProtocolType.CLIENT;

public class ClientChannelInitializer
        extends AbstractChannelInitializer {

    ClientChannelInitializer(ServerContext serverContext, EndpointConfig config) {
        super(serverContext, config);
    }

    @Override
    public void initChannel(Channel channel) {
        ServerConnection connection = (TcpServerConnection) channel.attributeMap().get(ServerConnection.class);
        MaintenanceLane maintenanceLane = serverContext.getClientEngine().getMaintenanceLane();
        if (maintenanceLane != null && !maintenanceLane.admitNewConnection()) {
            // load protection (F23): refused before any client work is done for it
            connection.close("Client connection refused by load protection", null);
            return;
        }
        ClientAdmission admission = serverContext.getClientEngine().getClientAdmission();
        SingleProtocolEncoder protocolEncoder = new SingleProtocolEncoder(
                new ClientMessageEncoder(admission == null ? -1 : admission.clientBufferBytes()));
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(
                CLIENT,
                new ClientMessageDecoder(connection, serverContext.getClientEngine(), serverContext.properties()),
                protocolEncoder);

        channel.outboundPipeline().addLast(protocolEncoder);
        channel.inboundPipeline().addLast(protocolDecoder);
        if (admission != null) {
            admission.usePooledBuffers(channel);
        }
    }
}
