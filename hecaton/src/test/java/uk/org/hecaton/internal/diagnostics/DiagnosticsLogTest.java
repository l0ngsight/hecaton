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
import uk.org.hecaton.internal.metrics.LongProbeFunction;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.ProbeLevel;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static uk.org.hecaton.internal.diagnostics.AbstractDiagnosticsPluginTest.cleanupDiagnosticFiles;
import static uk.org.hecaton.test.Accessors.getMetricsRegistry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class DiagnosticsLogTest extends HecatonTestSupport {

    private Diagnostics diagnostics;
    private DiagnosticsLogFile diagnosticsLogFile;
    private MetricsRegistry metricsRegistry;

    @After
    public void teardown() {
        cleanupDiagnosticFiles(diagnostics);
    }

    @Test
    public void testLogFileContent() {
        setup(10);
        assertTrueEventually(() -> {
            String content = loadLogfile(diagnosticsLogFile.file);
            assertNotNull(content);

            assertContains(content, "SystemProperties[");
            assertContains(content, "BuildInfo[");
            assertContains(content, "ConfigProperties[");
            assertContains(content, "Metric[");
        });
    }

    @Test
    public void testRollover() {
        // 200KB
        setup(0.2f);
        // we register 50 probes to quickly fill up the diagnostics log file
        String id = generateRandomString(10000);
        LongProbeFunction<Object> probe = source -> 0;
        for (int k = 0; k < 50; k++) {
            metricsRegistry.registerStaticProbe(this, id + k, ProbeLevel.MANDATORY, probe);
        }

        // we run for some time to make sure we get enough rollovers
        final List<File> files = new LinkedList<>();
        while (files.size() < 3) {
            final File file = diagnosticsLogFile.file;
            if (file != null) {
                if (!files.contains(file)) {
                    files.add(file);
                }

                assertTrueEventually(() -> assertExist(file));
            }

            sleepMillis(100);
        }

        // eventually all these files should be gone
        assertTrueEventually(() -> {
            for (File file : files) {
                assertNotExist(file);
            }
        });
    }

    private void setup(float maxFileSizeMB) {
        Config config = new Config();
        config.setProperty(Diagnostics.ENABLED.getName(), "true");
        config.setProperty(Diagnostics.MAX_ROLLED_FILE_COUNT.getName(), "3");
        config.setProperty(Diagnostics.MAX_ROLLED_FILE_SIZE_MB.getName(), String.valueOf(maxFileSizeMB));
        config.setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");

        HecatonInstance hz = createHecatonInstance(config);

        diagnostics = AbstractDiagnosticsPluginTest.getDiagnostics(hz);
        diagnosticsLogFile = (DiagnosticsLogFile) diagnostics.diagnosticsLog;
        metricsRegistry = getMetricsRegistry(hz);
    }


    private static String loadLogfile(File file) {
        if (file == null || !file.exists()) {
            return null;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertExist(File file) {
        assertTrue("file " + file.getAbsolutePath() + " should exist", file.exists());
    }

    private static void assertNotExist(File file) {
        assertFalse("file " + file.getAbsolutePath() + " should not exist", file.exists());
    }
}
