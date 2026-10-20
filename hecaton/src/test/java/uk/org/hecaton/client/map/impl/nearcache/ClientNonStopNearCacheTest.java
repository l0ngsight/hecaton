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

package uk.org.hecaton.client.map.impl.nearcache;

import uk.org.hecaton.client.HecatonClientOfflineException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientConnectionStrategyConfig;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientNonStopNearCacheTest extends ClientTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();
    private IMap<Object, Object> map;

    @Before
    public void init() {
        HecatonInstance member = factory.newHecatonInstance();
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Integer.MAX_VALUE);

        NearCacheConfig nearCacheConfig = new NearCacheConfig("map");
        config.addNearCacheConfig(nearCacheConfig);

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        config.addListenerConfig(new ListenerConfig().setImplementation((LifecycleListener) event -> {
            if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                disconnectedLatch.countDown();
            }
        }));
        HecatonInstance client = factory.newHecatonClient(config);

        map = client.getMap("map");


        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        //populate near cache
        for (int i = 0; i < 100; i++) {
            map.get(i);
        }

        member.shutdown();

        assertOpenEventually(disconnectedLatch);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testClusterShutdown_getExistingKeysFromCache() {
        //verify the near cache data available while client is disconnected
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), i);
        }
    }

    @Test(expected = HecatonClientOfflineException.class)
    public void testClusterShutdown_getNonExistingKeyFromCache() {
        //verify that if client ask for non available key, we get offline exception immediately
        map.get(200);
    }

    @Test(expected = HecatonClientOfflineException.class)
    public void testClusterShutdown_putToMap() {
        //verify that if client tries to put a map with cache when disconnected, we get offline exception immediately
        map.put(1, 2);
    }

}
