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

package uk.org.hecaton.internal.server.tcp;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.impl.CapturingCollector;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_BINDADDRESS;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_ENDPOINT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_COUNT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_CONNECTION;
import static uk.org.hecaton.internal.metrics.ProbeUnit.COUNT;
import static uk.org.hecaton.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.HecatonTestSupport.assertClusterSize;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
@Ignore("https://github.com/hazelcast/hazelcast/issues/18877")
public class TcpMetricValuesTest {

    @Before
    @After
    public void after() {
        HecatonInstanceFactory.terminateAll();
    }

    @Test
    public void membersDefinedViaIps() {
        test("127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703");
    }

    @Test
    public void membersDefinedViaHostname() {
        test("localhost:5701", "localhost:5702", "localhost:5703");
    }

    private void test(String... members) {
        int timeoutSecs = 10;
        // start 3 member cluster
        final HecatonInstance hz1 = Hecaton.newHecatonInstance(getConfig(members));
        final HecatonInstance hz2 = Hecaton.newHecatonInstance(getConfig(members));
        final HecatonInstance hz3 = Hecaton.newHecatonInstance(getConfig(members));
        assertClusterSize(3, hz1, hz2, hz3);
        assertTrueEventually(() -> {
            CapturingCollector collector = new CapturingCollector();
            getNodeEngineImpl(hz1).getMetricsRegistry().collect(collector);

            verifyMetricValue(collector, metricDescriptor(TCP_METRIC_ENDPOINT_MANAGER_COUNT), 2);
            verifyMetricValue(collector, metricDescriptor(TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT), 2);
            verifyNoOfCollectedMetrics(collector, TCP_DISCRIMINATOR_ENDPOINT, 2);
            verifyNoOfCollectedMetrics(collector, TCP_DISCRIMINATOR_BINDADDRESS, 2);
        }, timeoutSecs);
    }

    private void verifyMetricValue(
            CapturingCollector collector,
            MetricDescriptor expectedDescriptor,
            long expected
    ) {
        CapturingCollector.Capture capture = collector.captures().get(expectedDescriptor);
        assertNotNull(capture);
        assertEquals(expectedDescriptor.toString(), 1, capture.hits());
        long actual = (long) capture.singleCapturedValue();
        assertEquals(expected, actual);
    }

    private void verifyNoOfCollectedMetrics(
            CapturingCollector collector,
            String discriminator,
            long expected
    ) {
        Map<MetricDescriptor, CapturingCollector.Capture> captures = collector.captures().entrySet().stream()
                .filter(e -> Objects.equals(e.getKey().prefix(), TCP_PREFIX_CONNECTION))
                .filter(e -> Objects.equals(e.getKey().discriminator(), discriminator))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        int actual = captures.size();
        assertEquals(String.format("Actual value of %d for %s is not equal to %d",
                        actual, discriminator, expected), expected, actual);
    }

    private MetricDescriptor metricDescriptor(String metric) {
        return DEFAULT_DESCRIPTOR_SUPPLIER.get()
                .withPrefix(TCP_PREFIX_CONNECTION)
                .withMetric(metric)
                .withUnit(COUNT);
    }

    private Config getConfig(String... members) {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        for (String member : members) {
            join.getTcpIpConfig().addMember(member);
        }
        return config;
    }
}
