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

package uk.org.hecaton.internal.server;

import uk.org.hecaton.auditlog.AuditlogService;
import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.MemcacheProtocolConfig;
import uk.org.hecaton.config.RestApiConfig;
import uk.org.hecaton.config.SymmetricEncryptionConfig;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.internal.ascii.TextCommandService;
import uk.org.hecaton.internal.networking.InboundHandler;
import uk.org.hecaton.internal.networking.OutboundHandler;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.nio.MemberSocketInterceptor;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.spi.properties.HecatonProperties;

import java.io.IOException;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Contains many of the dependencies passed to the {@link Server}.
 */
@SuppressWarnings("checkstyle:methodcount")
public interface ServerContext {

    int KILO_BYTE = 1024;

    boolean isNodeActive();

    HecatonProperties properties();

    String getHecatonName();

    LoggingService getLoggingService();

    // returns the MEMBER server socket address
    Address getThisAddress();

    /**
     * Returns UUID of the local member.
     *
     * @return member UUID
     */
    UUID getThisUuid();

    /**
     * @return all server socket addresses of this Hecaton member, as picked by the
     * configured {@link uk.org.hecaton.instance.AddressPicker}
     */
    Map<EndpointQualifier, Address> getThisAddresses();

    void onFatalError(Exception e);

    SymmetricEncryptionConfig getSymmetricEncryptionConfig(EndpointQualifier endpointQualifier);

    /**
     * Returns initialized {@link RestApiConfig} for the node.
     */
    RestApiConfig getRestApiConfig();

    /**
     * Returns initialized {@link MemcacheProtocolConfig} for the node.
     */
    MemcacheProtocolConfig getMemcacheProtocolConfig();

    ClientEngine getClientEngine();

    TextCommandService getTextCommandService();

    void removeEndpoint(Address endpointAddress);

    void onSuccessfulConnection(Address address);

    void onFailedConnection(Address address);

    void shouldConnectTo(Address address);

    void interceptSocket(EndpointQualifier endpointQualifier, Socket socket, boolean onAccept) throws IOException;

    boolean isSocketInterceptorEnabled(EndpointQualifier endpointQualifier);

    int getSocketConnectTimeoutSeconds(EndpointQualifier endpointQualifier);

    long getConnectionMonitorInterval();

    int getConnectionMonitorMaxFaults();

    void onDisconnect(Address endpoint, Throwable cause);

    Future<Void> submitAsync(Runnable runnable);

    EventService getEventService();

    Collection<Integer> getOutboundPorts(EndpointQualifier endpointQualifier);

    InternalSerializationService getSerializationService();

    MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier);

    InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, ServerConnection connection);

    OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier, ServerConnection connection);

    AuditlogService getAuditLogService();
}
