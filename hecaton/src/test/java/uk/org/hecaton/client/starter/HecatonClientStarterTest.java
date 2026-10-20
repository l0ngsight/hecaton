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

package uk.org.hecaton.client.starter;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.starter.HecatonStarter;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class HecatonClientStarterTest {

    private HecatonInstance memberInstance;

    @After
    public void tearDown() {
        if (memberInstance != null) {
            memberInstance.shutdown();
        }
    }

    @Test
    public void testClientLifecycle() {
        memberInstance = HecatonStarter.newHecatonInstance("4.0.3");

        for (int i = 1; i < 4; i++) {
            String version = "4.0." + i;
            System.out.println("Starting client " + version);
            HecatonInstance instance = HecatonClientStarter.newHecatonClient(version, false);
            System.out.println("Stopping client " + version);
            instance.shutdown();
        }
    }

    @Test
    public void testClientMap() {
        HecatonInstance clientInstance = null;
        try {
            memberInstance = HecatonStarter.newHecatonInstance("4.0.3");
            clientInstance = HecatonClientStarter.newHecatonClient("4.0.3", false);

            IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
            IMap<Integer, Integer> memberMap = memberInstance.getMap("myMap");

            clientMap.put(1, 2);

            assertEquals(2, (int) memberMap.get(1));
        } finally {
            if (clientInstance != null) {
                clientInstance.shutdown();
            }
        }
    }

    @Test
    public void testAdvancedClientMap() {
        memberInstance = HecatonStarter.newHecatonInstance("4.0.3");
        HecatonInstance clientInstance = HecatonClientStarter.newHecatonClient("4.0.3", false);

        System.out.println("About to terminate the client");
        clientInstance.getLifecycleService().terminate();
        System.out.println("Client terminated");
    }
}
