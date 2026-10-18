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

package uk.org.hecaton.client.util;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.FutureUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static uk.org.hecaton.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static uk.org.hecaton.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.STARTING;
import static uk.org.hecaton.test.TimeConstants.MINUTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientStateListenerTest extends ClientTestSupport {
    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test(timeout = MINUTE * 10)
    public void testWithUnusedClientConfig()
            throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        assertFalse(listener.awaitConnected(1, MILLISECONDS));

        assertFalse(listener.awaitDisconnected(1, MILLISECONDS));

        assertFalse(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertFalse(listener.isStarted());

        assertEquals(STARTING, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientASYNCStartConnected() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);

        hecatonFactory.newHecatonInstance();

        hecatonFactory.newHecatonClient(clientConfig);

        assertTrue(listener.awaitConnected());

        assertFalse(listener.awaitDisconnected(1, MILLISECONDS));

        assertTrue(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertTrue(listener.isStarted());

        assertEquals(CLIENT_CONNECTED, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientReconnectModeAsyncConnected() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        hecatonFactory.newHecatonClient(clientConfig);

        hecatonInstance.shutdown();

        assertTrue(listener.awaitDisconnected());

        assertFalse(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertTrue(listener.isStarted());

        assertEquals(CLIENT_DISCONNECTED, listener.getCurrentState());

        hecatonFactory.newHecatonInstance();

        assertTrue(listener.awaitConnected());

        assertTrue(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertTrue(listener.isStarted());

        assertEquals(CLIENT_CONNECTED, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientReconnectModeAsyncConnectedMultipleThreads() {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        ClientConfig clientConfig = new ClientConfig();
        final ClientStateListener listener = new ClientStateListener(clientConfig);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        hecatonFactory.newHecatonClient(clientConfig);

        hecatonInstance.shutdown();

        List<Future<?>> futures = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(() -> {
                try {
                    assertTrue(listener.awaitDisconnected());
                } catch (InterruptedException e) {
                    fail("Should not be interrupted");
                }

                assertFalse(listener.isConnected());

                assertFalse(listener.isShutdown());

                assertTrue(listener.isStarted());

                assertEquals(CLIENT_DISCONNECTED, listener.getCurrentState());
            }));
        }

        FutureUtil.waitForever(futures);
        assertTrue(FutureUtil.allDone(futures));

        hecatonFactory.newHecatonInstance();

        futures.clear();
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(() -> {
                try {
                    listener.awaitConnected();
                } catch (InterruptedException e) {
                    fail("Should not be interrupted");
                }

                assertTrue(listener.isConnected());

                assertFalse(listener.isShutdown());

                assertTrue(listener.isStarted());

                assertEquals(CLIENT_CONNECTED, listener.getCurrentState());
            }));
        }

        FutureUtil.waitForever(futures);
        assertTrue(FutureUtil.allDone(futures));
    }

    @Test(timeout = MINUTE * 10)
    public void testClientReconnectModeOffDisconnected() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        final ClientStateListener listener = new ClientStateListener(clientConfig);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);

        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        hecatonFactory.newHecatonClient(clientConfig);

        hecatonInstance.shutdown();

        hecatonFactory.newHecatonInstance();

        assertTrue(listener.awaitDisconnected());

        assertFalse(listener.isConnected());

        assertTrueEventually(() -> assertEquals(SHUTDOWN, listener.getCurrentState()));

        assertFalse(listener.isStarted());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientConnectedWithTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        hecatonFactory.newHecatonInstance();

        hecatonFactory.newHecatonClient(clientConfig);
        assertTrue(listener.awaitConnected());

        assertFalse(listener.awaitDisconnected(1, MILLISECONDS));

        assertTrue(listener.isConnected());

        assertFalse(listener.isShutdown());

        assertTrue(listener.isStarted());

        assertEquals(CLIENT_CONNECTED, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientConnectedWithoutTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        hecatonFactory.newHecatonInstance();

        hecatonFactory.newHecatonClient(clientConfig);

        listener.awaitConnected();

        assertEquals(CLIENT_CONNECTED, listener.getCurrentState());

        assertTrue(listener.isConnected());

        assertTrue(listener.isStarted());

        assertFalse(listener.awaitDisconnected(1, MILLISECONDS));

        assertFalse(listener.isShutdown());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientDisconnectedWithTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        hecatonFactory.newHecatonInstance();

        hecatonFactory.newHecatonClient(clientConfig).shutdown();

        assertFalse(listener.awaitConnected(1, MILLISECONDS));

        assertTrue(listener.awaitDisconnected(1, MILLISECONDS));

        assertFalse(listener.isConnected());

        assertTrue(listener.isShutdown());

        assertFalse(listener.isStarted());

        assertEquals(SHUTDOWN, listener.getCurrentState());
    }

    @Test(timeout = MINUTE * 10)
    public void testClientDisconnectedWithoutTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        ClientStateListener listener = new ClientStateListener(clientConfig);

        hecatonFactory.newHecatonInstance();

        hecatonFactory.newHecatonClient(clientConfig).shutdown();

        listener.awaitDisconnected();

        assertFalse(listener.awaitConnected(1, MILLISECONDS));

        assertFalse(listener.isConnected());

        assertTrue(listener.isShutdown());

        assertFalse(listener.isStarted());

        assertEquals(SHUTDOWN, listener.getCurrentState());
    }

}
