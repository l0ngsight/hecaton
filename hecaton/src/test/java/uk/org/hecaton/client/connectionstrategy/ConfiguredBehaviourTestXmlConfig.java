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

package uk.org.hecaton.client.connectionstrategy;

import uk.org.hecaton.client.config.ClientClasspathXmlConfig;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class})
public class ConfiguredBehaviourTestXmlConfig extends ClientTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private static final CountDownLatch asyncStartFromXmlLatch = new CountDownLatch(1);

    private static class AsyncStartListener implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == CLIENT_CONNECTED) {
                asyncStartFromXmlLatch.countDown();
            }
        }
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testAsyncStartTrueXmlConfig() {
        ClientClasspathXmlConfig clientConfig = new ClientClasspathXmlConfig(
                "hecaton-client-connection-strategy-asyncStart-true.xml");

        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        hecatonFactory.newHecatonInstance();

        assertOpenEventually(asyncStartFromXmlLatch);

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCSingleMemberStartLateXmlConfig() {

        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        ClientClasspathXmlConfig clientConfig = new ClientClasspathXmlConfig(
                "hecaton-client-connection-strategy-asyncReconnect.xml");

        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        hecatonInstance.shutdown();
        hecatonFactory.newHecatonInstance();

        assertOpenEventually(reconnectListener.reconnectedLatch);

        client.getMap(randomMapName());
    }
}
