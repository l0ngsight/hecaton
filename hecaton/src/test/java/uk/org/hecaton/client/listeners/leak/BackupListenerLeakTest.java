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

package uk.org.hecaton.client.listeners.leak;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.ClientEngineImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.connection.tcp.TcpClientConnectionManager;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.UUID;
import java.util.function.LongConsumer;

import static uk.org.hecaton.client.impl.clientside.ClientTestUtil.getHecatonClientInstanceImpl;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BackupListenerLeakTest {

    protected final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testBackupListenerRemoved_afterClientShutdown() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance hecaton = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        client.shutdown();
        Map<UUID, LongConsumer> backupListeners = ((ClientEngineImpl) getNode(hecaton).clientEngine).getBackupListeners();
        assertTrueEventually(() -> assertEquals(0, backupListeners.size()));
    }

    @Test
    public void testBackupListenerIsNotRemoved_afterClientRestart() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance hecaton = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        HecatonClientInstanceImpl clientImpl = getHecatonClientInstanceImpl(client);
        TcpClientConnectionManager connectionManager = (TcpClientConnectionManager) clientImpl.getConnectionManager();

        connectionManager.reset();

        Map<UUID, LongConsumer> backupListeners = ((ClientEngineImpl) getNode(hecaton).clientEngine).getBackupListeners();
        assertTrueEventually(() -> assertEquals(1, backupListeners.size()));
    }
}
