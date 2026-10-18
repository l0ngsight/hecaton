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

package uk.org.hecaton.internal.metrics.impl;

import uk.org.hecaton.internal.metrics.LongProbeFunction;
import uk.org.hecaton.internal.metrics.ProbeLevel;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.metrics.ProbeLevel.DEBUG;
import static uk.org.hecaton.internal.metrics.ProbeLevel.INFO;
import static uk.org.hecaton.internal.metrics.ProbeLevel.MANDATORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeLevelTest extends HecatonTestSupport {

    private ILogger logger;

    @Before
    public void setup() {
        logger = Logger.getLogger(MetricsRegistryImpl.class);
    }

    @Test
    public void test() {
        assertProbeExist(MANDATORY, MANDATORY);
        assertNotProbeExist(INFO, MANDATORY);
        assertNotProbeExist(DEBUG, MANDATORY);

        assertProbeExist(MANDATORY, INFO);
        assertProbeExist(INFO, INFO);
        assertNotProbeExist(DEBUG, INFO);

        assertProbeExist(MANDATORY, DEBUG);
        assertProbeExist(INFO, DEBUG);
        assertProbeExist(DEBUG, DEBUG);
    }

    public void assertProbeExist(ProbeLevel probeLevel, ProbeLevel minimumLevel) {
        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(logger, minimumLevel);

        metricsRegistry.registerStaticProbe(this, "foo", probeLevel, (LongProbeFunction<ProbeLevelTest>) source -> 10);

        assertContains(metricsRegistry.getNames(), "[metric=foo]");
        assertEquals(10, metricsRegistry.newLongGauge("foo").read());
    }

    public void assertNotProbeExist(ProbeLevel probeLevel, ProbeLevel minimumLevel) {
        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(logger, minimumLevel);

        metricsRegistry.registerStaticProbe(this, "foo", probeLevel, (LongProbeFunction<ProbeLevelTest>) source -> 10);

        assertFalse(metricsRegistry.getNames().contains("[metric=foo]"));
        assertEquals(0, metricsRegistry.newLongGauge("foo").read());
    }
}
