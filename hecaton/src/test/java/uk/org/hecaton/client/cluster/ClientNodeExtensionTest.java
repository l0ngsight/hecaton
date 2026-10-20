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

package uk.org.hecaton.client.cluster;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.instance.impl.HecatonNodeExtension;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientNodeExtensionTest extends HecatonTestSupport {
    private TestHecatonFactory factory;

    @Before
    public void before() {
        factory = new TestHecatonFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(expected = IllegalStateException.class)
    public void test_canNotConnect_whenNodeExtensionIsNotComplete() throws UnknownHostException {
        factory.withNodeExtensionCustomizer(node -> new MockNodeExtension(node, false))
               .newHecatonInstance(new Address("127.0.0.1", 5555));

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5555");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), "1000");
        factory.newHecatonClient(clientConfig);
    }

    @Test(expected = OperationTimeoutException.class)
    public void test_canGetFromMap_whenNodeExtensionIsNotComplete() {
        IMap<Object, Object> map = null;
        AtomicReference<MockNodeExtension> nodeExtension = new AtomicReference<>();
        try {
            factory.withNodeExtensionCustomizer(node -> {
                nodeExtension.set(new MockNodeExtension(node, true));
                return nodeExtension.get();
            }).newHecatonInstance(new Address("127.0.0.1", 5555));

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
            clientConfig.getNetworkConfig().addAddress("127.0.0.1:5555");

            HecatonInstance hecatonClient = factory.newHecatonClient(clientConfig);

            map = hecatonClient.getMap(randomMapName());

            assertNull(map.get("dummy"));
        } catch (Throwable t) {
            fail("Should not throw exception! Error:" + t);
        }

        nodeExtension.get().setStartupDone(false);

        map.get("dummy");
    }

    private static class MockNodeExtension extends HecatonNodeExtension {
        private final AtomicBoolean startupDone;

        MockNodeExtension(Node node, boolean isStarted) {
            super(node);
            startupDone = new AtomicBoolean(isStarted);
        }

        @Override
        public boolean isStartCompleted() {
            return startupDone.get() && super.isStartCompleted();
        }

        public void setStartupDone(boolean started) {
            this.startupDone.set(started);
        }
    }
}
