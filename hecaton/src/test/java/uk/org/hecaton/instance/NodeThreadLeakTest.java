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

package uk.org.hecaton.instance;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.instance.impl.DefaultNodeContext;
import uk.org.hecaton.instance.impl.DefaultNodeExtension;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.server.Server;
import uk.org.hecaton.internal.server.tcp.LocalAddressRegistry;
import uk.org.hecaton.internal.server.tcp.ServerSocketRegistry;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static classloading.ThreadLeakTestUtils.assertHecatonThreadShutdown;
import static classloading.ThreadLeakTestUtils.getThreads;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests that an exception in the {@link Node} and {@link NodeEngineImpl} constructor leads to properly finished services.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class NodeThreadLeakTest extends HecatonTestSupport {

    private static final HecatonException HECATON_EXCEPTION
            = new HecatonException("Test exception - Emulates service failure.");
    private static final String RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG = "%s should be created before throwing exception to check resource cleanup";
    private static final int TEST_TIMEOUT_IN_MILLIS = 30_000;

    @Test(timeout = TEST_TIMEOUT_IN_MILLIS)
    public void testLeakWhenCreatingConnectionManager() {
        testFailingHecatonCreation(new DefaultNodeContext() {
            @Override
            public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
                assertThat(node.getNodeEngine())
                        .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "Node.nodeEngine")
                        .isNotNull();
                super.createServer(node, registry, addressRegistry);
                throw HECATON_EXCEPTION;
            }
        });
    }

    @Test(timeout = TEST_TIMEOUT_IN_MILLIS)
    public void testFailingInNodeEngineImplConstructor() {
        testFailingHecatonCreation(new DefaultNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                return new DefaultNodeExtension(node) {
                    @Override
                    public <T> T createService(Class<T> clazz, Object... params) {
                        NodeEngineImpl nodeEngine = (NodeEngineImpl) params[0];
                        assertNodeEnginePartiallyCreated(nodeEngine);
                        throw HECATON_EXCEPTION;
                    }
                };
            }
        });
    }

    private static void assertNodeEnginePartiallyCreated(NodeEngineImpl nodeEngine) {
        assertThat(nodeEngine.getExecutionService())
                .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "NodeEngineImpl.executionService")
                .isNotNull();
        assertThat(nodeEngine.getEventService())
                .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "NodeEngineImpl.eventService")
                .isNotNull();
        assertThat(nodeEngine.getOperationParker())
                .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "NodeEngineImpl.operationParker")
                .isNotNull();
        assertThat(nodeEngine.getOperationService())
                .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "NodeEngineImpl.operationService")
                .isNotNull();
    }

    private void testFailingHecatonCreation(DefaultNodeContext nodeContext) {

        Set<Thread> threads = getThreads();
        try {
            Config config = new Config();
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

            HecatonInstanceFactory.newHecatonInstance(
                    config,
                    config.getInstanceName(),
                    nodeContext
            );
            fail("Starting the member should have failed");
        } catch (HecatonException expected) {
            ignore(expected);
        }

        assertHecatonThreadShutdown("There are still Hecaton threads running after failed service initialization!", threads);
    }
}
