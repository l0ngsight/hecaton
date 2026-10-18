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

package uk.org.hecaton.client.impl.spi;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.HecatonTestSupport.randomMapName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientProxyDestroyTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance client;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient();
    }

    @Test
    public void testUsageAfterDestroy() {
        ISet proxy = newClientProxy();
        proxy.destroy();
        proxy.size();
    }

    @Test
    public void testMultipleDestroyCalls() {
        DistributedObject proxy = newClientProxy();
        proxy.destroy();
        proxy.destroy();
    }

    private ISet<Object> newClientProxy() {
        return client.getSet(HecatonTestSupport.randomString());
    }

    @Test
    public void testOperationAfterDestroy() {
        final String mapName = randomMapName();
        final IMap<Object, Object> clientMap = client.getMap(mapName);
        clientMap.destroy();
        assertFalse(client.getDistributedObjects().contains(clientMap));
        clientMap.put(1, 1);
        assertEquals(1, clientMap.get(1));
    }
}
