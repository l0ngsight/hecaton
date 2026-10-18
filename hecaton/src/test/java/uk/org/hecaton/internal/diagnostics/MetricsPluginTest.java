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
import uk.org.hecaton.internal.metrics.Probe;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.metrics.MetricTarget.DIAGNOSTICS;
import static uk.org.hecaton.internal.metrics.ProbeLevel.MANDATORY;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import org.junit.After;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class MetricsPluginTest extends AbstractDiagnosticsPluginTest {

    private MetricsPlugin plugin;
    private MetricsRegistry metricsRegistry;
    private Diagnostics diagnostics;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(Diagnostics.ENABLED.getName(), "true")
                .setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");
        HecatonInstance hz = createHecatonInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        metricsRegistry = nodeEngineImpl.getMetricsRegistry();
        plugin = new MetricsPlugin(nodeEngineImpl.getLogger(MetricsPlugin.class),
                nodeEngineImpl.getMetricsRegistry(), nodeEngineImpl.getProperties());
        plugin.onStart();

        diagnostics = AbstractDiagnosticsPluginTest.getDiagnostics(hz);
    }

    @After
    public void teardown() {
        cleanupDiagnosticFiles(diagnostics);
    }

    @Test
    public void testGetPeriodMillis() {
        long periodMillis = plugin.getPeriodMillis();
        assertEquals(SECONDS.toMillis(1), periodMillis);
    }

    @Test
    public void testRunWithProblematicProbe() {
        metricsRegistry.registerStaticProbe(this, "broken", MANDATORY, (LongProbeFunction) source -> {
            throw new RuntimeException("error");
        });

        plugin.run(logWriter);
        assertContains("[metric=broken]=java.lang.RuntimeException:error");
    }

    @Test
    public void testRun() {
        plugin.run(logWriter);

        // we just test a few to make sure the metrics are written
        assertContains("[unit=count,metric=client.endpoint.count]=0");
        assertContains("[unit=count,metric=operation.responseQueueSize]=0");
    }

    @Test
    public void testExclusion() {
        metricsRegistry.registerStaticMetrics(new ExclusionProbeSource(), "test");

        plugin.run(logWriter);

        assertContains("[unit=count,metric=test.notExcludedLong]=1");
        assertNotContains("[unit=count,metric=test.excludedLong]=1");
        assertContains("[unit=count,metric=test.notExcludedDouble]=1.5");
        assertNotContains("[unit=count,metric=test.excludedDouble]=2.5");
    }

    private static class ExclusionProbeSource {
        @Probe(name = "notExcludedLong")
        private long notExcludedLong = 1;

        @Probe(name = "excludedLong", excludedTargets = DIAGNOSTICS)
        private long excludedLong = 2;

        @Probe(name = "notExcludedDouble")
        private double notExcludedDouble = 1.5D;

        @Probe(name = "excludedDouble", excludedTargets = DIAGNOSTICS)
        private double excludedDouble = 2.5D;
    }

}
