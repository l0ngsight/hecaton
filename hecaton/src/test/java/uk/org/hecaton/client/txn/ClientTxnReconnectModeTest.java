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

import uk.org.hecaton.client.HecatonClientNotActiveException;
import uk.org.hecaton.client.HecatonClientOfflineException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientConnectionStrategyConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionException;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnReconnectModeTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Parameterized.Parameter
    public boolean allMembersRouting;

    @Parameterized.Parameters(name = "allMembersRouting:{0} ")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test(expected = OperationTimeoutException.class)
    public void testNewTransactionContext_ReconnectMode_ON() throws Throwable {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(allMembersRouting
                ? RoutingMode.ALL_MEMBERS : RoutingMode.SINGLE_MEMBER);
        config.getConnectionStrategyConfig().setAsyncStart(true);
        config.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        config.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ON);
        HecatonInstance client = hecatonFactory.newHecatonClient(config);
        try {
            client.newTransactionContext();
        } catch (TransactionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = HecatonClientOfflineException.class)
    public void testNewTransactionContext_ReconnectMode_ASYNC() throws Throwable {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(allMembersRouting
                ? RoutingMode.ALL_MEMBERS : RoutingMode.SINGLE_MEMBER);
        config.getConnectionStrategyConfig().setAsyncStart(true);

        config.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
        HecatonInstance client = hecatonFactory.newHecatonClient(config);
        try {
            client.newTransactionContext();
        } catch (TransactionException e) {
            throw e.getCause();
        }
    }

    @Test(expected = HecatonClientNotActiveException.class)
    public void testNewTransactionContext_After_shutdown() throws Throwable {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(allMembersRouting
                ? RoutingMode.ALL_MEMBERS : RoutingMode.SINGLE_MEMBER);
        config.getConnectionStrategyConfig().setAsyncStart(true);

        config.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
        HecatonInstance client = hecatonFactory.newHecatonClient(config);
        client.shutdown();
        try {
            client.newTransactionContext();
        } catch (TransactionException e) {
            throw e.getCause();
        }
    }
}
