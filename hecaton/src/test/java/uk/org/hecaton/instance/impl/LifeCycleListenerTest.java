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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleEvent.LifecycleState;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class LifeCycleListenerTest extends HecatonTestSupport {

    @Test(timeout = 15 * 1000)
    public void testListenerNoDeadLock() throws Exception {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final CountDownLatch latch = new CountDownLatch(1);
        final Config config = new Config();
        config.addListenerConfig(new ListenerConfig(new MyLifecycleListener(latch)));
        factory.newHecatonInstance(config);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testListenerInvocationWhenNodeStarts() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final Config config = new Config();
        final EventCountingListener listener = new EventCountingListener();
        config.addListenerConfig(new ListenerConfig(listener));
        factory.newHecatonInstance(config);
        assertEquals(LifecycleState.STARTING, listener.events.get(0));
        assertEquals(LifecycleState.STARTED, listener.events.get(1));
    }

    @Test
    public void testListenerInvocationWhenNodeShutsDown() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final Config config = new Config();
        final EventCountingListener listener = new EventCountingListener();
        config.addListenerConfig(new ListenerConfig(listener));
        HecatonInstance instance = factory.newHecatonInstance(config);

        listener.events.clear();
        instance.getLifecycleService().shutdown();
        assertEquals(LifecycleState.SHUTTING_DOWN, listener.events.get(0));
        assertEquals(LifecycleState.SHUTDOWN, listener.events.get(1));
    }

    @Test
    public void testListenerInvocationWhenNodeTerminates() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final Config config = new Config();
        final EventCountingListener listener = new EventCountingListener();
        config.addListenerConfig(new ListenerConfig(listener));
        HecatonInstance instance = factory.newHecatonInstance(config);

        listener.events.clear();
        instance.getLifecycleService().terminate();
        assertEquals(LifecycleState.SHUTTING_DOWN, listener.events.get(0));
        assertEquals(LifecycleState.SHUTDOWN, listener.events.get(1));
    }

    static class MyLifecycleListener implements LifecycleListener {

        private final CountDownLatch latch;

        MyLifecycleListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleState.STARTED) {
                Hecaton.getHecatonInstanceByName("_hzInstance_1_dev");
                latch.countDown();
            }
        }
    }

    static class EventCountingListener implements LifecycleListener {

        private final List<LifecycleState> events = new CopyOnWriteArrayList<>();

        @Override
        public void stateChanged(LifecycleEvent event) {
            events.add(event.getState());
        }

    }

}
