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

package uk.org.hecaton.jet.core;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.jet.Observable;
import uk.org.hecaton.jet.impl.execution.DoneItem;
import uk.org.hecaton.ringbuffer.OverflowPolicy;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.test.annotation.Repeat;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class ObservableRingbufferContainerLeakTest extends JetTestSupport {

    /**
     * Regression test for #19580
     */
    @Test
    @Repeat(100)
    public void testLeakingRingbufferContainerWhenUsingObservableIterator() {
        Config config = smallInstanceConfig();
        config.setProperty("hecaton.partition.count", "1");
        config.getJetConfig().setEnabled(true);

        HecatonInstance hz = createHecatonInstance(config);
        JetService jet = hz.getJet();

        Ringbuffer<Object> ringbuffer = hz.getRingbuffer("__jet.observables.my-observable");

        Observable<Object> obs = jet.getObservable("my-observable");
        ringbuffer.add(42);
        ringbuffer.addAsync(DoneItem.DONE_ITEM, OverflowPolicy.OVERWRITE);

        Iterator<Object> it = obs.iterator();
        it.hasNext();
        it.next();
        it.hasNext();

        obs.destroy();

        RingbufferService ringbufferService = Accessors.getService(hz, RingbufferService.SERVICE_NAME);

        Map<ObjectNamespace, RingbufferContainer> containers = ringbufferService
                .getContainers()
                .values()
                .iterator()
                .next(); // We set partition count to 1 so there will be only 1 item

        assertThat(containers).isEmpty();
    }
}
