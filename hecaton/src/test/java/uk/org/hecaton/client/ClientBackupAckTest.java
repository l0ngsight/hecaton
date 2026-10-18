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

package uk.org.hecaton.client;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.impl.spi.EventHandler;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationServiceImpl;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.util.ConfigRoutingUtil;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static uk.org.hecaton.client.config.RoutingMode.ALL_MEMBERS;
import static uk.org.hecaton.client.config.RoutingMode.MULTI_MEMBER;
import static uk.org.hecaton.client.config.RoutingMode.SINGLE_MEMBER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientBackupAckTest extends ClientTestSupport {

    @Parameterized.Parameter
    public RoutingMode routingMode;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(SINGLE_MEMBER, RoutingMode.ALL_MEMBERS);
    }

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testBackupAckToClientIsEnabled_byDefault() {
        Assume.assumeTrue(routingMode == RoutingMode.ALL_MEMBERS);

        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = newClientConfig();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertTrue(isEnabled(client));
    }

    @Test
    public void testBackupAckToClientIsEnabled() {
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = newClientConfig();
        clientConfig.setBackupAckToClientEnabled(true);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        boolean backupAckToClientEnabled
                = getHecatonClientInstanceImpl(client)
                .getInvocationService().isBackupAckToClientEnabled();

        if (routingMode == SINGLE_MEMBER || routingMode == MULTI_MEMBER) {
            assertFalse(isEnabled(client));
            assertFalse(backupAckToClientEnabled);
        } else if (routingMode == ALL_MEMBERS) {
            assertTrue(isEnabled(client));
            assertTrue(backupAckToClientEnabled);
        } else {
            throw new IllegalStateException("Unexpected value: " + routingMode);
        }
    }

    @Test
    public void testBackupAckToClientIsDisabled() {
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = newClientConfig();
        clientConfig.setBackupAckToClientEnabled(false);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertFalse(isEnabled(client));
    }

    private boolean isEnabled(HecatonInstance client) {
        Collection<EventHandler> values = getAllEventHandlers(client).values();
        for (EventHandler value : values) {
            if (value instanceof ClientInvocationServiceImpl.BackupEventHandler) {
                return true;
            }
        }
        return false;
    }

    private ClientConfig newClientConfig() {
        ClientConfig clientConfig = ConfigRoutingUtil.newClientConfig(routingMode);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        return clientConfig;
    }
}
