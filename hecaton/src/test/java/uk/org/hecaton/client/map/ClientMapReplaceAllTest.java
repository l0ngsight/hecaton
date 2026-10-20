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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.starter.HecatonStarter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import testsubjects.StaticSerializableBiFunction;
import testsubjects.StaticSerializableBiFunctionEx;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientMapReplaceAllTest extends HecatonTestSupport {

    private HecatonInstance memberInstance;

    protected HecatonInstance clientInstance;

    @After
    public final void stopHecatonInstances() {
        if (memberInstance != null) {
            memberInstance.shutdown();
        }
        HecatonClient.shutdownAll();
    }

    @Before
    public final void startHecatonInstances() {
        Config config = getConfig();
        MapConfig mapConfig = new MapConfig("mapConfig");
        config.addMapConfig(mapConfig);
        ClientConfig clientConfig = getClientConfig();
        memberInstance = HecatonStarter.newHecatonInstance("4.0.3");
        clientInstance = HecatonClient.newHecatonClient();
    }

    @Test
    public void testReplaceAllWithStaticSerializableFunction() {
        IMap<String, String> map = clientInstance.getMap(UuidUtil.newUnsecureUuidString());
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.replaceAll(new StaticSerializableBiFunction("v_new"));
        assertEquals("v_new", map.get("k1"));
        assertEquals("v_new", map.get("k2"));
    }

    @Test
    public void testReplaceAllWithLambdaFunction() {
        IMap<String, Integer> map = clientInstance.getMap(UuidUtil.newUnsecureUuidString());
        map.put("k1", 1);
        map.put("k2", 2);
        map.replaceAll((k, v) -> v * 10);
        assertEquals(10, (int) map.get("k1"));
        assertEquals(20, (int) map.get("k2"));
    }

    @Test(expected = ArithmeticException.class)
    public void testReplaceAllWithStaticSerializableFunction_ThrowsException() {
        IMap<Integer, Integer> map = clientInstance.getMap(randomString());
        map.put(1, 0);
        map.replaceAll(new StaticSerializableBiFunctionEx());
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
