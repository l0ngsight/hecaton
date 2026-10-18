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

package uk.org.hecaton.ringbuffer.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.diagnostics.StoreLatencyPlugin;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.ringbuffer.RingbufferStore;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LatencyTrackingRingbufferStoreTest extends HecatonTestSupport {

    private static final String NAME = "LatencyTrackingRingbufferStoreTest";
    private static final ObjectNamespace NAMESPACE = RingbufferService.getRingbufferNamespace(NAME);

    private StoreLatencyPlugin plugin;
    private RingbufferStore<String> delegate;
    private LatencyTrackingRingbufferStore<String> ringbufferStore;

    @Before
    public void setup() {
        HecatonInstance hz = createHecatonInstance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        plugin = new StoreLatencyPlugin(nodeEngine.getLogger(StoreLatencyPlugin.class), nodeEngine.getProperties());
        delegate = mock(RingbufferStore.class);
        ringbufferStore = new LatencyTrackingRingbufferStore<>(delegate, plugin,
                NAMESPACE);
    }

    @Test
    public void load() {
        long sequence = 1L;
        String value = "someValue";

        when(delegate.load(sequence)).thenReturn(value);

        String result = ringbufferStore.load(sequence);

        assertEquals(value, result);
        assertProbeCalledOnce("load");
    }

    @Test
    public void getLargestSequence() {
        long largestSequence = 100L;
        when(delegate.getLargestSequence()).thenReturn(largestSequence);

        long result = ringbufferStore.getLargestSequence();

        assertEquals(largestSequence, result);
        assertProbeCalledOnce("getLargestSequence");
    }

    @Test
    public void store() {
        long sequence = 1L;
        String value = "value1";

        ringbufferStore.store(sequence, value);

        verify(delegate).store(sequence, value);
        assertProbeCalledOnce("store");
    }

    @Test
    public void storeAll() {
        String[] items = new String[]{"1", "2"};

        ringbufferStore.storeAll(100, items);

        verify(delegate).storeAll(100, items);
        assertProbeCalledOnce("storeAll");
    }

    public void assertProbeCalledOnce(String methodName) {
        assertEquals(1, plugin.count(LatencyTrackingRingbufferStore.KEY,
                NAMESPACE.getServiceName() + ":" + NAMESPACE.getObjectName(), methodName));
    }
}
