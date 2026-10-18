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
import uk.org.hecaton.internal.crdt.AbstractCRDTBounceTest;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class PNCounterBounceTest extends AbstractCRDTBounceTest {

    private static final String TEST_PN_COUNTER_NAME = "counter";
    private static final ILogger LOGGER = Logger.getLogger(PNCounterBounceTest.class);

    private final AtomicLong assertionCounter = new AtomicLong();
    private final Random rnd = new Random();

    @Override
    protected void mutate(HecatonInstance hecatonInstance) {
        final int delta = rnd.nextInt(100) - 50;
        hecatonInstance.getPNCounter(TEST_PN_COUNTER_NAME).addAndGet(delta);
        assertionCounter.addAndGet(delta);
    }

    @Override
    protected void assertState(final HecatonInstance hecatonInstance) {
        assertTrueEventually(() -> assertEquals(assertionCounter.get(), hecatonInstance.getPNCounter(TEST_PN_COUNTER_NAME).get()));
    }

    @Override
    protected ILogger getLogger() {
        return LOGGER;
    }
}
