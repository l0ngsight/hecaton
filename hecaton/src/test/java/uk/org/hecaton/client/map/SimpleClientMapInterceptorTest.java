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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.helpers.PortableHelpersFactory;
import uk.org.hecaton.client.helpers.SimpleClientInterceptor;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.InterceptorTest;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.UuidUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SimpleClientMapInterceptorTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance client;
    private HecatonInstance server1;
    private HecatonInstance server2;

    private SimpleClientInterceptor interceptor;

    @Before
    public void setup() {
        Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(PortableHelpersFactory.ID, new PortableHelpersFactory());
        server1 = hecatonFactory.newHecatonInstance(config);
        server2 = hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PortableHelpersFactory.ID, new PortableHelpersFactory());
        client = hecatonFactory.newHecatonClient(clientConfig);

        interceptor = new SimpleClientInterceptor();
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void clientMapInterceptorTestIssue1238() {
        final IMap<Object, Object> map = client.getMap("clientMapInterceptorTest");

        String id = map.addInterceptor(interceptor);

        map.put(1, "New York");
        map.put(2, "Istanbul");
        map.put(3, "Tokyo");
        map.put(4, "London");
        map.put(5, "Paris");
        map.put(6, "Cairo");
        map.put(7, "Hong Kong");

        map.remove(1);

        try {
            map.remove(2);
            fail();
        } catch (Exception ignore) {

        }

        assertEquals(6, map.size());
        assertNull(map.get(1));
        assertEquals("ISTANBUL:", map.get(2));
        assertEquals("TOKYO:", map.get(3));
        assertEquals("LONDON:", map.get(4));
        assertEquals("PARIS:", map.get(5));
        assertEquals("CAIRO:", map.get(6));
        assertEquals("HONG KONG:", map.get(7));

        map.removeInterceptor(id);
        map.put(8, "Moscow");

        assertEquals("Moscow", map.get(8));
        assertNull(map.get(1));
        assertEquals("ISTANBUL", map.get(2));
        assertEquals("TOKYO", map.get(3));
        assertEquals("LONDON", map.get(4));
        assertEquals("PARIS", map.get(5));
        assertEquals("CAIRO", map.get(6));
        assertEquals("HONG KONG", map.get(7));
    }

    @Test
    public void removeInterceptor_returns_true_when_interceptor_removed() {
        String mapName = "mapWithInterceptor";
        IMap map = client.getMap(mapName);
        String id = map.addInterceptor(new InterceptorTest.SimpleInterceptor());

        assertTrue(map.removeInterceptor(id));
        assertNoRegisteredInterceptorExists(server1.getMap(mapName));
        assertNoRegisteredInterceptorExists(server2.getMap(mapName));
    }

    @Test
    public void removeInterceptor_returns_false_when_there_is_no_interceptor() {
        String mapName = "mapWithNoInterceptor";
        IMap map = client.getMap(mapName);

        assertFalse(map.removeInterceptor(UuidUtil.newUnsecureUuidString()));
        assertNoRegisteredInterceptorExists(server1.getMap(mapName));
        assertNoRegisteredInterceptorExists(server2.getMap(mapName));
    }

    private static void assertNoRegisteredInterceptorExists(IMap map) {
        String mapName = map.getName();
        MapService mapservice = (MapService) (((MapProxyImpl) map).getService());
        mapservice.getMapServiceContext().getMapContainer(mapName).getInterceptorRegistry().getInterceptors();
    }

}
