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
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class BuildInfoPluginTest extends AbstractDiagnosticsPluginTest {

    private BuildInfoPlugin plugin;

    @Before
    public void setup() {
        HecatonInstance hz = createHecatonInstance();
        plugin = new BuildInfoPlugin(getNodeEngineImpl(hz).getLogger(BuildInfoPlugin.class));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(DiagnosticsPlugin.RUN_ONCE_PERIOD_MS, plugin.getPeriodMillis());
    }

    @Test
    public void test() {
        plugin.run(logWriter);

        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

        assertContains("BuildNumber=" + buildInfo.getBuildNumber());
        assertContains("Build=" + buildInfo.getBuild());
        assertContains("Revision=" + buildInfo.getRevision());
        assertContains("Version=" + buildInfo.getVersion());
        assertContains("SerialVersion=" + buildInfo.getSerializationVersion());
        assertContains("Enterprise=" + buildInfo.isEnterprise());
    }
}
