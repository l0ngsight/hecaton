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

package uk.org.hecaton.cluster;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeShutdownEventsTest extends HecatonTestSupport {

    /**
     * When a node fails during join, it will be shut down.
     * In that scenario we are expecting lifecycle events (SHUTTING_DOWN & SHUTDOWN)
     * to be fired locally.
     */
    @Test
    public void testNodeShutdown_firesLifecycleEvents_afterJoinFailure() {
        // Only expecting SHUTTING_DOWN & SHUTDOWN events so latch count should be 2.
        final CountDownLatch shutdownEventCount = new CountDownLatch(2);

        // Having different partition counts is cause of the join failure.
        Config config1 = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "111");

        Config config2 = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "222");

        final ListenerConfig listenerConfig = new ListenerConfig();
        listenerConfig.setImplementation((LifecycleListener) event -> {
            // Only expecting SHUTTING_DOWN & SHUTDOWN.
            if (LifecycleEvent.LifecycleState.SHUTTING_DOWN == event.getState()
                    || LifecycleEvent.LifecycleState.SHUTDOWN == event.getState()) {
                shutdownEventCount.countDown();
            }
        });
        config2.addListenerConfig(listenerConfig);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        factory.newHecatonInstance(config1);

        try {
            factory.newHecatonInstance(config2);
            fail("Second node should fail during join");
        } catch (IllegalStateException e) {
            // ignore IllegalStateException since we are only testing lifecycle events.
        }

        assertOpenEventually(shutdownEventCount);
    }
}
