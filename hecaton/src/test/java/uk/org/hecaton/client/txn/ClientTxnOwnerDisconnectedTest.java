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

package uk.org.hecaton.client.txn;

import com.atomikos.icatch.jta.UserTransactionManager;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.transaction.HecatonXAResource;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.Transaction;
import java.io.File;
import java.util.concurrent.CountDownLatch;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientTxnOwnerDisconnectedTest extends ClientTestSupport {

    @After
    public void after() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Before
    public void before() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test(expected = TransactionException.class)
    public void testTransactionBeginShouldFail_onDisconnectedState() {
        Hecaton.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);

        Hecaton.newHecatonInstance();
        final TransactionContext context = client.newTransactionContext();

        final CountDownLatch clientDisconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> clientDisconnected.countDown());

        Hecaton.shutdownAll();

        assertOpenEventually(clientDisconnected);

        context.beginTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testNewTransactionContextShouldFail_onDisconnectedState() {
        Hecaton.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);

        Hecaton.newHecatonInstance();

        final CountDownLatch clientDisconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> clientDisconnected.countDown());

        Hecaton.shutdownAll();

        assertOpenEventually(clientDisconnected);

        client.newTransactionContext();
    }

    @Test(expected = TransactionException.class)
    public void testXAShouldFail_onDisconnectedState() throws Throwable {
        Hecaton.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);

        Hecaton.newHecatonInstance();


        HecatonXAResource xaResource = client.getXAResource();
        UserTransactionManager tm = new UserTransactionManager();
        cleanAtomikosLogs();
        tm.setTransactionTimeout(3);
        tm.begin();
        Transaction transaction = tm.getTransaction();

        final CountDownLatch clientDisconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> clientDisconnected.countDown());

        Hecaton.shutdownAll();

        assertOpenEventually(clientDisconnected);

        try {
            transaction.enlistResource(xaResource);
        } finally {
            transaction.rollback();
            tm.close();
            cleanAtomikosLogs();
        }
    }

    public void cleanAtomikosLogs() {
        try {
            File currentDir = new File(".");
            final File[] tmLogs = currentDir.listFiles((dir, name) -> name.endsWith(".epoch") || name.startsWith("tmlog"));
            for (File tmLog : tmLogs) {
                tmLog.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
