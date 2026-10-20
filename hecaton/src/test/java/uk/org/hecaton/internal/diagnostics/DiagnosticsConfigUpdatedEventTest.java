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
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.management.events.DiagnosticsConfigUpdatedEvent;
import uk.org.hecaton.internal.management.events.Event;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DiagnosticsConfigUpdatedEventTest extends AbstractDiagnosticsPluginTest {

    private List<HecatonInstance> hecatonInstances = new ArrayList<>();
    private TestHecatonInstanceFactory instanceFactory;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setup() {
        instanceFactory = createHecatonInstanceFactory();
        HecatonInstance[] instances = instanceFactory.newInstances(regularInstanceConfig(), 3);
        hecatonInstances.addAll(Arrays.asList(instances));
    }

    @Test
    public void testDiagnosticsConfigUpdatedEvent() throws IOException {

        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(true)
                .setAutoOffDurationInMinutes(1)
                .setProperty(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "100")
                .setLogDirectory(folder.newFolder().getAbsolutePath())
                .setFileNamePrefix("hz")
                .setMaxRolledFileSizeInMB(9)
                .setOutputType(DiagnosticsOutputType.STDOUT)
                .setIncludeEpochTime(true);

        // assert for enabled
        CountDownLatch latchForEnabled = new CountDownLatch(hecatonInstances.size());
        List<Event> eventsForEnabled = new ArrayList<>();
        setListener(latchForEnabled, eventsForEnabled);
        setDiagnosticsConfig(diagnosticsConfig);
        assertEventEmittedEventually(diagnosticsConfig, latchForEnabled, eventsForEnabled);


        // assert for disabled
        CountDownLatch latchForDisabled = new CountDownLatch(hecatonInstances.size());
        List<Event> eventsForDisabled = new ArrayList<>();
        setListener(latchForDisabled, eventsForDisabled);
        diagnosticsConfig.setEnabled(false);
        setDiagnosticsConfig(diagnosticsConfig);
        assertEventEmittedEventually(diagnosticsConfig, latchForDisabled, eventsForDisabled);
    }

    @Test
    public void testDiagnosticsConfigUpdatedEvent_whenNewMemberJoins() throws IOException {

        DiagnosticsConfig diagnosticsConfig = null;
        try {
            diagnosticsConfig = new DiagnosticsConfig()
                    .setEnabled(true)
                    .setAutoOffDurationInMinutes(1)
                    .setProperty(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "100")
                    .setLogDirectory(folder.newFolder().getAbsolutePath())
                    .setFileNamePrefix("hz")
                    .setMaxRolledFileSizeInMB(9)
                    .setOutputType(DiagnosticsOutputType.FILE)
                    .setIncludeEpochTime(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // assert for enabled
        CountDownLatch latchForEnabled = new CountDownLatch(hecatonInstances.size());
        List<Event> eventsForEnabled = new ArrayList<>();
        setListener(latchForEnabled, eventsForEnabled);
        setDiagnosticsConfig(diagnosticsConfig);

        assertEventEmittedEventually(diagnosticsConfig, latchForEnabled, eventsForEnabled);

        // Add new member and combine the list
        hecatonInstances.add(instanceFactory.newHecatonInstance(regularInstanceConfig()));

        assertTrueEventually(() -> assertAllInSafeState(hecatonInstances));

        // verify all members have diagnostics enabled
        for (HecatonInstance hecatonInstance : hecatonInstances) {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(hecatonInstance);
            assertTrue(hecatonInstance.getName() + " not enabled the diagnostics.", nodeEngine.getDiagnostics().isEnabled());
        }
    }

    private void setListener(CountDownLatch latch, List<Event> events) {
        for (HecatonInstance hecatonInstance : hecatonInstances) {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(hecatonInstance);
            ManagementCenterService mcService = nodeEngine.getManagementCenterService();
            mcService.setEventListener(event -> {
                events.add(event);
                latch.countDown();
            });
        }
    }

    private void assertEventEmittedEventually(DiagnosticsConfig expCfg, CountDownLatch latch, List<Event> events) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        List<String> uuids = new ArrayList<>(hecatonInstances.stream()
                .map(Accessors::getNodeEngineImpl)
                .map(nodeEngine -> nodeEngine.getLocalMember().getUuid().toString())
                .toList());


        // All members should emit the event after applying the config since each have separated Diagnostics service

        for (Event event : events) {
            if (event instanceof DiagnosticsConfigUpdatedEvent dcEvent) {
                JsonObject eventCfg = dcEvent.toJson();

                assertEquals(expCfg.isEnabled(), eventCfg.getBoolean("enabled", false));
                assertEquals(expCfg.getMaxRolledFileSizeInMB(), eventCfg.getFloat("max-rolled-file-size-in-mb", 0f), 0.1f);
                assertEquals(expCfg.getMaxRolledFileCount(), eventCfg.getInt("max-rolled-file-count", 0));
                assertEquals(expCfg.isIncludeEpochTime(), eventCfg.getBoolean("include-epoch-time", false));
                assertEquals(expCfg.getLogDirectory(), eventCfg.getString("log-directory", ""));
                assertEquals(expCfg.getFileNamePrefix(), eventCfg.getString("file-name-prefix", ""));
                assertEquals(expCfg.getOutputType().name(), eventCfg.getString("output-type", ""));
                assertEquals(expCfg.getAutoOffDurationInMinutes(), eventCfg.getInt("auto-off-timer-in-minutes", 0));

                uuids.remove(eventCfg.getString("member-id", ""));

                JsonObject eventPluginProperties = eventCfg.get("plugin-properties").asObject();
                for (String key : expCfg.getPluginProperties().keySet()) {
                    assertEquals(expCfg.getPluginProperties().get(key),
                            eventPluginProperties.getString(key, ""));
                }
            }
        }

        assertTrue(uuids.isEmpty());
    }

    private void setDiagnosticsConfig(DiagnosticsConfig diagnosticsConfig) {
        ((DynamicConfigurationAwareConfig) hecatonInstances.get(0).getConfig()).setDiagnosticsConfig(diagnosticsConfig);
    }
}
