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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class SystemPropertiesPluginTest extends AbstractDiagnosticsPluginTest {

    private static final String FAKE_PROPERTY = "hecaton.fake.property";
    private static final String FAKE_PROPERTY_VALUE = "foobar";

    private SystemPropertiesPlugin plugin;

    @Before
    public void setup() {
        HecatonInstance hz = createHecatonInstance();
        plugin = new SystemPropertiesPlugin(getNodeEngineImpl(hz).getLogger(SystemPropertiesPlugin.class));
        plugin.onStart();
        System.setProperty(FAKE_PROPERTY, "foobar");
    }

    @After
    public void tearDown() {
        System.clearProperty(FAKE_PROPERTY);
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(DiagnosticsPlugin.RUN_ONCE_PERIOD_MS, plugin.getPeriodMillis());
    }

    @Test
    public void testRun() {
        plugin.run(logWriter);

        Properties systemProperties = System.getProperties();

        // we check a few of the regular ones
        assertContains("java.class.version=" + systemProperties.get("java.class.version"));
        assertContains("java.class.path=" + systemProperties.get("java.class.path"));

        // we want to make sure the hecaton system properties are added
        assertContains(FAKE_PROPERTY + "=" + FAKE_PROPERTY_VALUE);

        // java.vm.args doesn't work under windows
        // https://github.com/hazelcast/hazelcast/issues/11610
        assertContains(SystemPropertiesPlugin.JVM_ARGS + "=");

        // we don't want to have awt
        assertNotContains("java.awt");
    }
}
