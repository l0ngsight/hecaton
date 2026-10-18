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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.ringbuffer.OverflowPolicy;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.test.AssertTask;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class RingbufferDestroyTest extends HecatonTestSupport {

    public static final String NAME = "ringbuffer-foo";

    private static HecatonInstance[] instances;
    private Ringbuffer<String> ringbuffer;

    @Before
    public void setUp() throws Exception {
        Config config = smallInstanceConfig();
        config.addRingBufferConfig(new RingbufferConfig(NAME).setCapacity(10));
        instances = createHecatonInstances(config, 2);

        ringbuffer = instances[0].getRingbuffer(NAME);
    }

    @Test
    public void whenDestroyAfterAdd_thenRingbufferRemoved() {
        ringbuffer.add("1");
        ringbuffer.destroy();

        assertTrueEventually(new AssertNoRingbufferContainerTask(), 10);
    }

    @Test
    public void whenDestroyAfterAddAllAsync_thenRingbufferRemoved() throws Exception {
        ringbuffer.addAllAsync(newArrayList("1"), OverflowPolicy.FAIL).toCompletableFuture().get();
        ringbuffer.destroy();

        assertTrueEventually(new AssertNoRingbufferContainerTask(), 10);
    }

    @Test
    public void whenReadOneAfterDestroy_thenMustNotRecreateRingbuffer() {
        ringbuffer.add("1");
        ringbuffer.destroy();

        spawn(() -> ringbuffer.readOne(0));

        sleepMillis(100);

        assertTrueEventually(new AssertNoRingbufferContainerTask(), 10);
    }

    @Test
    public void whenReadManyAfterDestroy_thenMustNotRecreateRingbuffer() {
        ringbuffer.add("1");
        ringbuffer.destroy();

        spawn(() -> ringbuffer.readManyAsync(0, 1, 1, null));

        sleepMillis(100);

        assertTrueEventually(new AssertNoRingbufferContainerTask(), 10);
    }

    private static class AssertNoRingbufferContainerTask implements AssertTask {
        @Override
        public void run() {
            for (HecatonInstance instance : instances) {
                final RingbufferService ringbufferService
                        = getNodeEngineImpl(instance).getService(RingbufferService.SERVICE_NAME);

                final Map<ObjectNamespace, RingbufferContainer> partitionContainers =
                        ringbufferService.getContainers().get(ringbufferService.getRingbufferPartitionId(NAME));
                assertNotNull(partitionContainers);
                assertFalse(partitionContainers.containsKey(RingbufferService.getRingbufferNamespace(NAME)));
            }
        }
    }
}
