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

package uk.org.hecaton.client.internal.metrics;

import uk.org.hecaton.client.Client;
import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.statistics.ClientStatistics;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricTarget;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.ProbeUnit;
import uk.org.hecaton.internal.metrics.impl.CapturingCollector;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;

import static uk.org.hecaton.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static uk.org.hecaton.internal.metrics.ProbeUnit.COUNT;
import static uk.org.hecaton.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMetricsTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testLongClientMetricIsMergedIntoMemberMetrics() {
        HecatonInstance memberInstance = givenMemberWithTwoClients();
        MetricsRegistry metricsRegistry = getNodeEngineImpl(memberInstance).getMetricsRegistry();

        // randomly chosen long client metric, we just check that
        // we have client-side metrics merged into the member metrics
        assertClientMetricsObservedEventually(memberInstance, metricsRegistry, "gc", "minorCount", COUNT);
    }

    @Test
    public void testDoubleClientMetricIsMergedIntoMemberMetrics() {
        HecatonInstance memberInstance = givenMemberWithTwoClients();
        MetricsRegistry metricsRegistry = getNodeEngineImpl(memberInstance).getMetricsRegistry();

        // randomly chosen double client metric, we just check that
        // we have client-side metrics merged into the member metrics
        assertClientMetricsObservedEventually(memberInstance, metricsRegistry, "os", "systemLoadAverage", null);
    }

    private HecatonInstance givenMemberWithTwoClients() {
        Config conf = getConfig();
        conf.getMetricsConfig()
            .setEnabled(true)
            .setCollectionFrequencySeconds(1);

        HecatonInstance memberInstance = hecatonFactory.newHecatonInstance(conf);
        hecatonFactory.newHecatonClient();
        hecatonFactory.newHecatonClient();
        return memberInstance;
    }

    private void assertClientMetricsObservedEventually(HecatonInstance memberInstance, MetricsRegistry metricsRegistry,
                                                       String prefix, String metric, ProbeUnit unit) {
        assertTrueEventually(() -> {
            CapturingCollector collector = new CapturingCollector();
            metricsRegistry.collect(collector);

            ClientEngine clientEngine = getNode(memberInstance).getClientEngine();
            Collection<Client> clients = clientEngine.getClients();

            assertEquals(2, clients.size());

            for (Client client : clients) {
                UUID clientUuid = client.getUuid();
                ClientStatistics clientStatistics = clientEngine.getClientStatistics().get(clientUuid);
                assertNotNull(clientStatistics);
                long timestamp = clientStatistics.timestamp();

                MetricDescriptor expectedDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                        .get()
                        .withPrefix(prefix)
                        .withMetric(metric)
                        .withUnit(unit)
                        .withTag("client", clientUuid.toString())
                        .withTag("clientname", client.getName())
                        .withTag("timestamp", Long.toString(timestamp))
                        .withExcludedTargets(asList(MetricTarget.values()))
                        .withIncludedTarget(MANAGEMENT_CENTER);

                assertContains(collector.captures().keySet(), expectedDescriptor);
            }
        });
    }

}
