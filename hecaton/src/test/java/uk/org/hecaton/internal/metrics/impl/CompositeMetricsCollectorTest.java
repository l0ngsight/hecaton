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

import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.collectors.MetricsCollector;
import uk.org.hecaton.mock.MockUtil;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static uk.org.hecaton.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.openMocks;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeMetricsCollectorTest {

    @Mock
    private MetricsCollector collectorMock1;

    @Mock
    private MetricsCollector collectorMock2;

    private MetricDescriptor metricsDescriptor;

    private CompositeMetricsCollector compositeCollector;

    private AutoCloseable openMocks;

    @Before
    public void setUp() {
        openMocks = openMocks(this);
        compositeCollector = new CompositeMetricsCollector(collectorMock1, collectorMock2);

        metricsDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("test")
                .withMetric("metric");
    }

    @After
    public void cleanUp() {
        MockUtil.closeMocks(openMocks);
    }

    @Test
    public void testCollectLong() {
        compositeCollector.collectLong(metricsDescriptor, 42);

        verify(collectorMock1).collectLong(metricsDescriptor, 42);
        verify(collectorMock2).collectLong(metricsDescriptor, 42);
    }

    @Test
    public void testCollectDouble() {
        compositeCollector.collectDouble(metricsDescriptor, 42.42D);

        verify(collectorMock1).collectDouble(metricsDescriptor, 42.42D);
        verify(collectorMock2).collectDouble(metricsDescriptor, 42.42D);
    }

    @Test
    public void testCollectException() {
        Exception ex = new Exception();
        compositeCollector.collectException(metricsDescriptor, ex);

        verify(collectorMock1).collectException(metricsDescriptor, ex);
        verify(collectorMock2).collectException(metricsDescriptor, ex);
    }

    @Test
    public void testCollectNoValue() {
        compositeCollector.collectNoValue(metricsDescriptor);

        verify(collectorMock1).collectNoValue(metricsDescriptor);
        verify(collectorMock2).collectNoValue(metricsDescriptor);
    }
}
