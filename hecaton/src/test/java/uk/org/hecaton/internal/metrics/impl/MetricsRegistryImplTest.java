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
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static uk.org.hecaton.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class MetricsRegistryImplTest extends HecatonTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    // ================ newLongGauge ======================

    @Test(expected = NullPointerException.class)
    public void newGauge_whenNullName() {
        metricsRegistry.newLongGauge(null);
    }

    @Test
    public void newGauge_whenNotExistingMetric() {
        LongGaugeImpl gauge = metricsRegistry.newLongGauge("foo");

        assertNotNull(gauge);
        assertEquals("foo", gauge.getName());
        assertEquals(0, gauge.read());
    }

    @Test
    public void newGauge_whenExistingMetric() {
        LongGaugeImpl first = metricsRegistry.newLongGauge("foo");
        LongGaugeImpl second = metricsRegistry.newLongGauge("foo");

        assertNotSame(first, second);
    }

    // ================ getNames ======================

    @Test
    public void getNames() {
        Set<String> metrics = new HashSet<>();
        metrics.add("first");
        metrics.add("second");
        metrics.add("third");

        Set<String> expected = new HashSet<>();
        expected.add("[metric=first]");
        expected.add("[metric=second]");
        expected.add("[metric=third]");

        for (String name : metrics) {
            metricsRegistry.registerStaticProbe(this, name, ProbeLevel.MANDATORY,
                    (LongProbeFunction<Object>) obj -> 0);
        }

        Set<String> names = metricsRegistry.getNames();
        for (String name : expected) {
            assertContains(names, name);
        }
    }

    @Test
    public void shutdown() {
        metricsRegistry.shutdown();
    }
}
