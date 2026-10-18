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
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.internal.util.phonehome.PhoneHome;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics.DIAGNOSTICS_DYNAMIC_AUTO_OFF_COUNT;
import static uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics.DIAGNOSTICS_DYNAMIC_ENABLED_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiagnosticsPhoneHomeTest extends AbstractDiagnosticsPluginTest {

    HecatonInstance hz;
    Diagnostics diagnostics;

    @Before
    public void setup() {
        Config config = new Config();
        hz = createHecatonInstance(config);
        diagnostics = TestUtil.getNode(hz).getNodeEngine().getDiagnostics();
        diagnostics.autoOffDurationUnit = TimeUnit.SECONDS;
    }

    @Test
    public void testDiagnosticsPhoneHome() {
        enableDiagnostics(-1);
        // Check if the phone home is enabled
        assertTrue(diagnostics.isEnabled());

        // Set the config to increase phone home metrics and autoOff count
        enableDiagnostics(5);

        assertTrueEventually(() -> {
            assertFalse(diagnostics.isEnabled());
        });

        PhoneHome phoneHome = new PhoneHome(TestUtil.getNode(hz));
        Map<String, String> result = phoneHome.phoneHome(true);

        assertEquals("2", result.get(DIAGNOSTICS_DYNAMIC_ENABLED_COUNT.getQueryParameter()));
        assertEquals("1", result.get(DIAGNOSTICS_DYNAMIC_AUTO_OFF_COUNT.getQueryParameter()));
    }

    private void enableDiagnostics(int duration) {
        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig();
        diagnosticsConfig.setEnabled(true);
        diagnosticsConfig.setAutoOffDurationInMinutes(duration);
        diagnostics.setConfig(diagnosticsConfig);
    }
}
