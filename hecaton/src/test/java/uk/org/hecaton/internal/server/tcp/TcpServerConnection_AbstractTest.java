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

import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.impl.MetricsRegistryImpl;
import uk.org.hecaton.internal.networking.nio.Select_NioNetworkingFactory;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.server.MockServerContext;
import uk.org.hecaton.internal.server.NetworkingFactory;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.server.TestDataFactory;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.impl.LoggingServiceImpl;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.instance.EndpointQualifier.MEMBER;
import static uk.org.hecaton.internal.metrics.ProbeLevel.INFO;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@SuppressWarnings("WeakerAccess")
public abstract class TcpServerConnection_AbstractTest extends HecatonTestSupport {

    private static final int PORT_NUMBER_UPPER_LIMIT = 5799;
    private int portNumber = 5701;

    protected NetworkingFactory networkingFactory = new Select_NioNetworkingFactory();

    protected ILogger logger;
    protected LoggingServiceImpl loggingService;
    protected InternalSerializationService serializationService;

    protected Address addressA;
    protected Address addressB;
    protected Address addressC;

    protected TcpServer tcpServerA;
    protected TcpServer tcpServerB;
    protected TcpServer tcpServerC;

    protected MockServerContext serverContextA;
    protected MockServerContext serverContextB;
    protected MockServerContext serverContextC;

    protected MetricsRegistryImpl metricsRegistryA;
    protected MetricsRegistryImpl metricsRegistryB;
    protected MetricsRegistryImpl metricsRegistryC;

    @Before
    public void setup() throws Exception {
        loggingService = new LoggingServiceImpl("somegroup", "log4j2", BuildInfoProvider.getBuildInfo(), true, false, null);
        logger = loggingService.getLogger(TcpServerConnection_AbstractTest.class);

        metricsRegistryA = newMetricsRegistry();

        tcpServerA = newMockTcpServer(metricsRegistryA);
        serverContextA = (MockServerContext) tcpServerA.getContext();
        addressA = serverContextA.getThisAddress();

        metricsRegistryB = newMetricsRegistry();
        tcpServerB = newMockTcpServer(metricsRegistryB);
        serverContextB = (MockServerContext) tcpServerB.getContext();
        addressB = serverContextB.getThisAddress();

        metricsRegistryC = newMetricsRegistry();
        tcpServerC = newMockTcpServer(metricsRegistryC);
        serverContextC = (MockServerContext) tcpServerC.getContext();
        addressC = serverContextC.getThisAddress();

        serializationService = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(TestDataFactory.FACTORY_ID, new TestDataFactory())
                .build();
    }

    @After
    public void tearDown() {
        tcpServerA.shutdown();
        tcpServerB.shutdown();
        tcpServerC.shutdown();

        metricsRegistryA.shutdown();
        metricsRegistryB.shutdown();
        metricsRegistryC.shutdown();
    }

    protected void startAllTcpServers() {
        tcpServerA.start();
        tcpServerB.start();
        tcpServerC.start();
    }

    protected MetricsRegistryImpl newMetricsRegistry() {
        return new MetricsRegistryImpl(loggingService.getLogger(MetricsRegistryImpl.class), INFO);
    }

    protected TcpServer newMockTcpServer(MetricsRegistry metricsRegistry) throws Exception {
        MockServerContext serverContext = null;
        while (serverContext == null) {
            try {
                serverContext = new MockServerContext(portNumber++, UuidUtil.newUnsecureUUID());
            } catch (IOException e) {
                if (portNumber >= PORT_NUMBER_UPPER_LIMIT) {
                    throw e;
                }
            }
        }

        ServerSocketRegistry registry = new ServerSocketRegistry(singletonMap(MEMBER, serverContext.serverSocketChannel), true);
        LocalAddressRegistry addressRegistry = new LocalAddressRegistry(logger);
        MockServerContext finalServiceContext = serverContext;
        return new TcpServer(null,
                serverContext,
                registry,
                addressRegistry,
                metricsRegistry,
                networkingFactory.create(serverContext, metricsRegistry),
                qualifier -> new UnifiedChannelInitializer(finalServiceContext));
    }

    // ====================== support ========================================

    protected TcpServerConnection connect(Address address) {
        return connect(tcpServerA, address);
    }

    protected TcpServerConnection connect(final TcpServer service, final Address address) {
        service.getConnectionManager(MEMBER).getOrConnect(address);

        final AtomicReference<TcpServerConnection> ref = new AtomicReference<>();
        assertTrueEventually(() -> {
            Connection c = service.getConnectionManager(MEMBER).get(address);
            assertNotNull(c);
            ref.set((TcpServerConnection) c);
        });

        return ref.get();
    }

    public static ServerConnection getConnection(TcpServer server, SocketAddress localSocketAddress) {
        long startMs = System.currentTimeMillis();

        for (; ; ) {
            for (ServerConnection connection : server.getConnectionManager(MEMBER).getConnections()) {
                if (connection.getRemoteSocketAddress().equals(localSocketAddress)) {
                    return connection;
                }
            }

            if (startMs + 20000 < System.currentTimeMillis()) {
                fail("Timeout: Could not find connection");
            }

            sleepMillis(100);
        }
    }
}
