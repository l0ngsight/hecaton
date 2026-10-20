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
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.instance.impl.HecatonNodeExtension;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.SimpleTokenCredentials;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientAuthenticationTest extends HecatonTestSupport {

    private TestHecatonFactory hecatonFactory;

    @Before
    public void setup() {
        hecatonFactory = new TestHecatonFactory();
    }

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test(expected = IllegalStateException.class)
    public void testNoClusterFound() {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        hecatonFactory.newHecatonClient(clientConfig);
    }

    @Test
    public void testAuthenticationWithCustomCredentials_when_singleNode() {
        DataSerializableFactory factory = new CustomCredentialsIdentifiedFactory();

        // with this config, the server will authenticate any credential of type CustomCredentials
        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactory(1, factory);
        hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials());

        // custom credentials are not supported when security is disabled on members
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        assertThatThrownBy(() -> hecatonFactory.newHecatonClient(clientConfig))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testAuthentication_with_mcModeEnabled() {
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hecaton.client.internal.mc.mode", "true");

        // if the client is able to connect, it's a pass
        hecatonFactory.newHecatonClient(clientConfig);
    }

    @Test
    public void testAuthentication_with_mcModeEnabled_when_clusterStart_isNotComplete() {
        hecatonFactory
                .withNodeExtensionCustomizer(node -> new HecatonNodeExtension(node) {
                    @Override
                    public boolean isStartCompleted() {
                        return false;
                    }
                })
                .newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hecaton.client.internal.mc.mode", "true");

        // if the client is able to connect, it's a pass
        hecatonFactory.newHecatonClient(clientConfig);
    }

    private static class CustomCredentialsIdentifiedFactory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int classId) {
            return new CustomCredentials();
        }
    }

    private static class CustomCredentials extends SimpleTokenCredentials {
        @Override
        public byte[] getToken() {
            return new byte[10];
        }
    }
}
