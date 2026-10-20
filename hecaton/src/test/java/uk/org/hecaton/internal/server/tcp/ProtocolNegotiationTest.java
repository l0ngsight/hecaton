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

import static uk.org.hecaton.internal.nio.IOUtil.close;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static java.lang.Math.max;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLSocket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.TestAwareInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;

/**
 * Verify that after sending member protocol header bytes (HCC) no more bytes are sent from the connection initiating member.
 * (Next bytes should only follow if the protocol is confirmed by the HCC reply - not tested by this test).
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({ QuickTest.class })
public class ProtocolNegotiationTest {

    private volatile BytesCountingServer bcServer;
    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @Before
    public void before() {
        bcServer = new BytesCountingServer(createServerSocket());
        new Thread(bcServer).start();
    }

    @After
    public void after() {
        factory.terminateAll();
        bcServer.stop();
        close(bcServer.serverSocket);
    }

    @Parameter
    public boolean advancedNetworking;

    @Parameters(name = "advancedNetworking:{0}")
    public static Object[] parameters() {
        return new Object[] { true, false };
    }

    /**
     * Verify that only 3 header bytes are sent to a server.
     */
    @Test
    public void verifyOnlyTheProtocolHeaderIsSent() {
        Config config = createConfig();
        try {
            factory.newHecatonInstance(config);
        } catch (IllegalStateException e) {
            // just ignore it. Usually the exception happens here, but not always (due to the tight 2s timing).
            // The result of the newHecatonInstance operation is not important as this test is focused on bytes sent to the
            // BytesCountingServer.
            e.printStackTrace();
        }
        bcServer.stop();
        assertEquals(3, bcServer.maxBytesReceived.get());
    }

    protected Config createConfig() {
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getAutoDetectionConfig().setEnabled(false);
        joinConfig.getTcpIpConfig().setEnabled(true).setConnectionTimeoutSeconds(3)
                .addMember("127.0.0.1:" + bcServer.serverSocket.getLocalPort());
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.INVOCATION_MAX_RETRY_COUNT.getName(), "1")
                .setProperty(ClusterProperty.INVOCATION_RETRY_PAUSE.getName(), "0")
                .setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0")
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "2000")
                .setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "2")
                ;
        if (advancedNetworking) {
            config.getAdvancedNetworkConfig().setEnabled(true).setJoin(joinConfig);
        } else {
            config.getNetworkConfig().setJoin(joinConfig);
        }
        return config;
    }

    protected ServerSocket createServerSocket() {
        try {
            return new ServerSocket(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static final class BytesCountingServer implements Runnable {
        private static final ILogger LOGGER = Logger.getLogger(BytesCountingServer.class);

        final ServerSocket serverSocket;
        volatile boolean shutdownRequested;
        final AtomicInteger maxBytesReceived = new AtomicInteger(-1);

        BytesCountingServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
            try {
                this.serverSocket.setSoTimeout(500);
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("The server will be listening on port " + serverSocket.getLocalPort());
        }

        @Override
        public void run() {
            try {
                while (!shutdownRequested) {
                    try {
                        Socket socket = serverSocket.accept();
                        new Thread(() -> {
                            LOGGER.info("Socket accepted " + socket);
                            try {
                                if (socket instanceof SSLSocket sslSocket) {
                                    sslSocket.startHandshake();
                                }
                                socket.setSoTimeout(100);
                                int count = readWithTimeout(socket.getInputStream(), 2000);
                                LOGGER.info("Bytes read: " + count);
                                maxBytesReceived.updateAndGet(c -> max(c, count));
                            } catch (IOException e) {
                                LOGGER.warning("Reading from the socket failed", e);
                            } finally {
                                close(socket);
                            }
                        }).start();
                    } catch (SocketTimeoutException e) {
                        // it's fine
                    }
                }
            } catch (IOException e) {
                LOGGER.warning("The test server thrown an exception", e);
            } finally {
                close(serverSocket);
            }
        }

        void stop() {
            shutdownRequested = true;
        }

        static int readWithTimeout(InputStream is, long timeoutMillis) throws IOException {
            int count = 0;
            long maxTimeMillis = System.currentTimeMillis() + timeoutMillis;
            while (System.currentTimeMillis() < maxTimeMillis) {
                try {
                    is.read();
                    count++;
                } catch (SocketTimeoutException e) {
                    // OK - we have the SO_TIMEOUT configured on the socket
                }
            }
            return count;
        }
    }

}
