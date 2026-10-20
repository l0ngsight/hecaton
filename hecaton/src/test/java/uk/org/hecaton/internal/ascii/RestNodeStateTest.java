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

package uk.org.hecaton.internal.ascii;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DiscoveryStrategyConfig;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.config.RestApiConfig;
import uk.org.hecaton.config.properties.PropertyDefinition;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeState;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.internal.util.EmptyStatement;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.discovery.AbstractDiscoveryStrategy;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.discovery.DiscoveryStrategy;
import uk.org.hecaton.spi.discovery.DiscoveryStrategyFactory;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static uk.org.hecaton.test.starter.ReflectionUtils.setFieldValueReflectively;
import static org.junit.Assert.assertEquals;

/**
 * Regression test which checks the {@link NodeState} before the instance becomes {@link NodeState#ACTIVE}.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class RestNodeStateTest {

    private static final int BASE_PORT = 5600;

    @Before
    @After
    public void cleanup() {
        HecatonInstanceFactory.terminateAll();
    }

    /**
     * This test does a node-state REST call before the Node is switched to the {@link NodeState#ACTIVE}. A custom discovery
     * strategy is used to block the processing in a point when REST is already running, but the node is not yet active. During
     * the blocked discovery initialization the HTTP call is done.
     * <p>
     * See
     * <a href="https://github.com/hazelcast/hazelcast/issues/17773">https://github.com/hazelcast/hazelcast/issues/17773</a>.
     */
    @Test
    public void testStartingNodeState_regression() throws Exception {
        Config config = smallInstanceConfig().setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        NetworkConfig networkConfig = config.getNetworkConfig();
        int port = TestUtil.getAvailablePort(BASE_PORT);
        networkConfig.setPort(port).setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.setRestApiConfig(restApiConfig);
        StrategyFactory discoveryStrategyFactory = new StrategyFactory();
        networkConfig.getJoin().getDiscoveryConfig()
                .addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(discoveryStrategyFactory));
        HecatonTestSupport.spawn(() -> Hecaton.newHecatonInstance(config));
        discoveryStrategyFactory.getNodeStartingLatch().await();
        HTTPCommunicator communicator = new HTTPCommunicator(port);
        assertEquals("\"STARTING\"", communicator.getClusterHealth("/node-state"));
        discoveryStrategyFactory.getTestDoneLatch().countDown();
    }

    @Test(timeout = 30000)
    public void testNodeStateAvailable_whenOtherMemberUnavailable() throws Exception {
        List<Integer> ports = TestUtil.getAvailablePorts(BASE_PORT, 2);
        Integer port1 = ports.get(0);
        Integer port2 = ports.get(1);
        Config config1 = config(port1);
        Config config2 = config(port2);
        HecatonInstance member1 = Hecaton.newHecatonInstance(config1);
        Hecaton.newHecatonInstance(config2);
        // make member1 fail operations with HecatonInstanceNotActiveException
        Node node1 = Accessors.getNode(member1);
        setFieldValueReflectively(node1, "state", NodeState.SHUT_DOWN);
        // query member2 about its node state
        // member shouldn't execute any cluster operation and respond immediately
        HTTPCommunicator communicator = new HTTPCommunicator(port2);
        assertEquals("\"ACTIVE\"", communicator.getClusterHealth("/node-state"));
    }

    private Config config(int port) {
        Config config = smallInstanceConfig();
        config.getNetworkConfig().getRestApiConfig().setEnabled(true).enableAllGroups();
        config.getNetworkConfig().setPort(port);
        return config;
    }

    public static class StrategyFactory implements DiscoveryStrategyFactory {

        final BlockingDiscoveryStrategy strategy = new BlockingDiscoveryStrategy();

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return BlockingDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                Map<String, Comparable> properties) {
            return strategy;
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return Collections.emptySet();
        }

        public CountDownLatch getNodeStartingLatch() {
            return strategy.nodeStartingLatch;
        }

        public CountDownLatch getTestDoneLatch() {
            return strategy.testDoneLatch;
        }
    }

    public static class BlockingDiscoveryStrategy extends AbstractDiscoveryStrategy {

        final CountDownLatch nodeStartingLatch = new CountDownLatch(1);
        final CountDownLatch testDoneLatch = new CountDownLatch(1);

        @Override
        public void start() {
            try {
                nodeStartingLatch.countDown();
                testDoneLatch.await();
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }

        public BlockingDiscoveryStrategy() {
            super(null, Collections.emptyMap());
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return Collections.emptyList();
        }
    }
}
