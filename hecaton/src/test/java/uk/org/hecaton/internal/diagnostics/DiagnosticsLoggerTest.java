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
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static uk.org.hecaton.instance.impl.TestUtil.getNode;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class DiagnosticsLoggerTest extends HecatonTestSupport {

    @Test
    public void testLoggerContent() {
        Config config = new Config()
                .setProperty(Diagnostics.ENABLED.getName(), "true")
                .setProperty(Diagnostics.OUTPUT_TYPE.getName(), DiagnosticsOutputType.LOGGER.name())
                .setProperty(Diagnostics.INCLUDE_EPOCH_TIME.getName(), "false")
                .setProperty(ClusterProperty.LOGGING_ENABLE_DETAILS.getName(), "false")
                .setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");

        HecatonInstance instance = createHecatonInstance(config);

        StringBuilder diagnosticsOutput = new StringBuilder();

        getNode(instance).loggingService.addLogListener(Level.FINE, event -> {
            LogRecord record = event.getLogRecord();
            if (record.getLoggerName().equals("uk.org.hecaton.diagnostics")) {
                diagnosticsOutput.append(record.getMessage());
            }
        });


        assertTrueEventually(() -> {
            String content = diagnosticsOutput.toString();
            assertNotNull(content);

            assertContains(content, "Metric[");
        });
    }
}
