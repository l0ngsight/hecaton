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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.TestNodeContext;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.ExpectedRuntimeException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static classloading.ThreadLeakTestUtils.assertHecatonThreadShutdown;
import static classloading.ThreadLeakTestUtils.getThreads;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

@RunWith(HecatonParallelClassRunner.class)
@Category(SlowTest.class)
public class HecatonInstanceFactoryTest extends HecatonTestSupport {

    private static Set<Thread> oldThreads;

    private HecatonInstance hecatonInstance;

    @BeforeClass
    public static void getThread() {
        oldThreads = getThreads();
    }

    @After
    public void tearDown() {
        if (hecatonInstance != null) {
            hecatonInstance.shutdown();
        }
    }

    @AfterClass
    public static void cleanUp() {
        HecatonInstanceFactory.terminateAll();

        assertHecatonThreadShutdown(oldThreads);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(HecatonInstanceFactory.class);
    }

    @Test
    public void testTestHecatonInstanceFactory() {
        TestHecatonInstanceFactory instanceFactory = new TestHecatonInstanceFactory();
        try {
            final HecatonInstance instance1 = instanceFactory.newHecatonInstance();
            final HecatonInstance instance2 = instanceFactory.newHecatonInstance();
            final HecatonInstance instance3 = instanceFactory.newHecatonInstance();

            assertClusterSizeEventually(3, instance1, instance2, instance3);
        } finally {
            instanceFactory.terminateAll();
        }
    }

    @Test
    public void testTestHecatonInstanceFactory_withTwoFactories() {
        TestHecatonInstanceFactory instanceFactory1 = new TestHecatonInstanceFactory();
        TestHecatonInstanceFactory instanceFactory2 = new TestHecatonInstanceFactory();
        try {
            final HecatonInstance instance11 = instanceFactory1.newHecatonInstance();
            final HecatonInstance instance12 = instanceFactory1.newHecatonInstance();
            final HecatonInstance instance13 = instanceFactory1.newHecatonInstance();

            final HecatonInstance instance21 = instanceFactory2.newHecatonInstance();
            final HecatonInstance instance22 = instanceFactory2.newHecatonInstance();

            assertClusterSizeEventually(2, instance21, instance22);
            assertClusterSizeEventually(3, instance11, instance12, instance13);
        } finally {
            instanceFactory1.terminateAll();
            instanceFactory2.terminateAll();
        }
    }

    @Test
    public void test_NewInstance_configLoaded() {
        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(null);

        assertNotNull(hecatonInstance.getConfig());
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failed_beforeNodeStart() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doThrow(new ExpectedRuntimeException()).when(nodeExtension).beforeStart();
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config, randomString(), context);
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failed_beforeJoin() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doThrow(new ExpectedRuntimeException()).when(nodeExtension).beforeJoin();
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config, randomString(), context);
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failed_afterNodeStart() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doThrow(new ExpectedRuntimeException()).when(nodeExtension).afterStart();
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config, randomString(), context);
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failed_beforeNodeShutdown() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doAnswer(new Answer() {
                    final AtomicBoolean throwException = new AtomicBoolean();

                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        if (throwException.compareAndSet(false, true)) {
                            throw new ExpectedRuntimeException();
                        }
                        return null;
                    }
                }).when(nodeExtension).beforeShutdown(true);
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config, randomString(), context);
        try {
            hecatonInstance.getLifecycleService().terminate();
        } catch (ExpectedRuntimeException expected) {
            hecatonInstance.getLifecycleService().terminate();
            throw expected;
        }
    }

    @Test(expected = IllegalStateException.class)
    public void test_NewInstance_terminateInstance_afterNodeStart() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(final Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);

                doAnswer(invocation -> {
                    node.hecatonInstance.shutdown();
                    return null;
                }).when(nodeExtension).afterStart();

                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config, randomString(), context);
    }

    @Test
    public void mobyNameGeneratedIfPropertyEnabled() {
        Config config = new Config();
        config.getProperties().put(ClusterProperty.MOBY_NAMING_ENABLED.getName(), "true");

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config);
        String name = hecatonInstance.getName();
        assertNotNull(name);
        assertNotContains(name, "_hzInstance_");
    }

    @Test
    public void fixedNameGeneratedIfPropertyDisabled() {
        Config config = new Config();
        config.getProperties().put(ClusterProperty.MOBY_NAMING_ENABLED.getName(), "false");

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config);
        String name = hecatonInstance.getName();
        assertNotNull(name);
        assertContains(name, "_hzInstance_");
    }

    @Test
    public void fixedNameGeneratedIfPropertyNotDefined() {
        Config config = new Config();

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config);
        String name = hecatonInstance.getName();
        assertNotNull(name);
        assertNotContains(name, "_hzInstance_");
    }

    @Test
    public void mobyNameGeneratedIfSystemPropertyEnabled() {
        Config config = new Config();
        ClusterProperty.MOBY_NAMING_ENABLED.setSystemProperty("true");

        hecatonInstance = HecatonInstanceFactory.newHecatonInstance(config);
        String name = hecatonInstance.getName();
        assertNotNull(name);
        assertNotContains(name, "_hzInstance_");
    }
}
