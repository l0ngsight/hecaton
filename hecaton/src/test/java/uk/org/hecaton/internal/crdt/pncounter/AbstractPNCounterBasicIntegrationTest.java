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

package uk.org.hecaton.internal.crdt.pncounter;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.internal.monitor.LocalPNCounterStats;
import uk.org.hecaton.internal.util.FutureUtil;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

/**
 * Base implementation for simple {@link PNCounter} tests.
 * Concrete implementations must provide the two {@link HecatonInstance}s
 * which will be used to perform the tests.
 */
public abstract class AbstractPNCounterBasicIntegrationTest extends HecatonTestSupport {

    @Test
    public void testSimpleReplication() {
        final PNCounter counter1 = getInstance1().getPNCounter("counter");
        final PNCounter counter2 = getInstance2().getPNCounter("counter");

        assertEquals(5, counter1.addAndGet(5));

        assertCounterValueEventually(5, counter1);
        assertCounterValueEventually(5, counter2);
    }

    @Test
    public void statistics() {
        final PNCounter counter1 = getInstance1().getPNCounter("counterWithStats");
        final PNCounter counter2 = getInstance2().getPNCounter("counterWithStats");
        final int parallelism = 5;
        final int loopsPerThread = 100;
        final AtomicLong finalValue = new AtomicLong();

        final ArrayList<Future> futures = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            futures.add(spawn((Runnable) () -> {
                for (int j = 0; j < loopsPerThread; j++) {
                    counter1.addAndGet(5);
                    finalValue.addAndGet(5);
                    counter2.addAndGet(-2);
                    finalValue.addAndGet(-2);
                }
            }));
        }
        FutureUtil.waitForever(futures);
        assertCounterValueEventually(finalValue.longValue(), counter1);
        assertCounterValueEventually(finalValue.longValue(), counter2);

        long increments = 0;
        long decrements = 0;
        for (HecatonInstance member : getMembers()) {
            final PNCounterService service = getNodeEngineImpl(member).getService(PNCounterService.SERVICE_NAME);
            for (LocalPNCounterStats stats : service.getStats().values()) {
                increments += stats.getTotalIncrementOperationCount();
                decrements += stats.getTotalDecrementOperationCount();
            }
        }

        assertEquals(parallelism * loopsPerThread, increments);
        assertEquals(parallelism * loopsPerThread, decrements);
    }

    protected abstract HecatonInstance getInstance1();

    protected abstract HecatonInstance getInstance2();

    protected abstract HecatonInstance[] getMembers();

    private void assertCounterValueEventually(final long expectedValue, final PNCounter counter) {
        assertTrueEventually(() -> assertEquals(expectedValue, counter.get()));
    }
}
