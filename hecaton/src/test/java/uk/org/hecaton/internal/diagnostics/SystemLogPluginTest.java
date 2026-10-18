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

package uk.org.hecaton.internal.diagnostics;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.diagnostics.DiagnosticsPlugin.NOT_SCHEDULED_PERIOD_MS;
import static uk.org.hecaton.internal.diagnostics.SystemLogPlugin.ENABLED;
import static uk.org.hecaton.internal.diagnostics.SystemLogPlugin.LOG_PARTITIONS;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("WeakerAccess")
public class SystemLogPluginTest extends AbstractDiagnosticsPluginTest {

    protected Config config;
    protected TestHecatonInstanceFactory hzFactory;
    protected HecatonInstance hz;
    protected SystemLogPlugin plugin;

    @Before
    public void setup() {
        config = new Config();
        config.setProperty(LOG_PARTITIONS.getName(), "true");

        hzFactory = createHecatonInstanceFactory(2);
        hz = hzFactory.newHecatonInstance(config);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        plugin = new SystemLogPlugin(
                nodeEngine.getLogger(SystemLogPlugin.class),
                nodeEngine.getProperties(),
                nodeEngine.getNode().getServer(),
                hz,
                nodeEngine.getNode().getNodeExtension());
        plugin.onStart();
    }

    @Test
    public void testGetPeriodSeconds() {
        assertEquals(1000, plugin.getPeriodMillis());
    }

    @Test
    public void testGetPeriodSeconds_whenPluginIsDisabled_thenReturnDisabled() {
        config.setProperty(ENABLED.getName(), "false");
        HecatonInstance instance = hzFactory.newHecatonInstance(config);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        plugin = new SystemLogPlugin(
                nodeEngine.getLogger(SystemLogPlugin.class),
                nodeEngine.getProperties(),
                nodeEngine.getNode().getServer(),
                instance,
                nodeEngine.getNode().getNodeExtension());
        plugin.onStart();

        assertEquals(NOT_SCHEDULED_PERIOD_MS, plugin.getPeriodMillis());
    }

    @Test
    public void testLifecycle() {
        hz.shutdown();

        assertTrueEventually(() -> {
            plugin.run(logWriter);

            assertContains("Lifecycle[" + System.lineSeparator() + "                          SHUTTING_DOWN]");
        });
    }

    @Test
    public void testMembership() {
        HecatonInstance instance = hzFactory.newHecatonInstance(config);
        assertTrueEventually(() -> {
            plugin.run(logWriter);
            assertContains("MemberAdded[");
        });

        instance.shutdown();
        assertTrueEventually(() -> {
            plugin.run(logWriter);
            assertContains("MemberRemoved[");
        });
    }

    @Test
    public void testMigration() {
        warmUpPartitions(hz);

        HecatonInstance instance = hzFactory.newHecatonInstance(config);
        warmUpPartitions(instance);

        waitAllForSafeState(hz, instance);

        assertTrueEventually(() -> {
            plugin.run(logWriter);
            assertContains("MigrationCompleted");
        });
    }
}
