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

package uk.org.hecaton.client.internal.diagnostics;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.diagnostics.AbstractDiagnosticsPluginTest;
import uk.org.hecaton.internal.diagnostics.SystemLogPlugin;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.diagnostics.SystemLogPlugin.LOG_PARTITIONS;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class SystemLogPluginConnectionTest extends AbstractDiagnosticsPluginTest {

    private TestHecatonFactory hzFactory;
    private SystemLogPlugin plugin;

    @Before
    public void setUp() {
        Config config = new Config();
        config.setProperty(LOG_PARTITIONS.getName(), "true");

        hzFactory = new TestHecatonFactory();
        HecatonInstance hz = hzFactory.newHecatonInstance(config);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        plugin = new SystemLogPlugin(
                nodeEngine.getLogger(SystemLogPlugin.class),
                nodeEngine.getProperties(),
                nodeEngine.getNode().getServer(),
                hz,
                nodeEngine.getNode().getNodeExtension());
        plugin.onStart();
    }

    @After
    public void tearDown() {
        hzFactory.terminateAll();
    }

    @Test
    public void testConnection() {
        HecatonInstance instance = hzFactory.newHecatonClient();
        assertTrueEventually(() -> {
            plugin.run(logWriter);
            assertContains("ConnectionAdded");
        });

        instance.shutdown();
        assertTrueEventually(() -> {
            plugin.run(logWriter);
            assertContains("ConnectionRemoved");
        });
    }
}
