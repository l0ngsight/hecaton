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

package uk.org.hecaton.client;


import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static org.junit.Assert.assertEquals;

/**
 * A test that verifies that a SINGLE_MEMBER routing client, can send request to a wrong node,
 * but still can get responses to its requests.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleMemberClientRoutingTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance client;
    private HecatonInstance server1;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        server1 = hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        client = hecatonFactory.newHecatonClient(clientConfig);
    }


    @Test
    public void test() {
        String mapName = randomString();
        // create some dummy data.
        Map<String, String> origin = new HashMap<>();
        for (int k = 0; k < 1000; k++) {
            String value = randomString();
            origin.put(value, value);
        }

        // insert that data on the server.
        server1.getMap(mapName).putAll(origin);

        // now iterate over that dummy data and make sure that we find everything that was inserted.
        IMap<String, String> map = client.getMap(mapName);
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            String key = entry.getKey();
            String expectedValue = map.get(key);
            String actualValue = map.get(key);
            assertEquals(expectedValue, actualValue);
        }
    }
}
