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
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.collection.IList;
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

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRedoEnabledTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance client;

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void init() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setRedoOperation(true);
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient(config);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testList_get_throwsIndexOutOfBound() {
        IList<Object> list = client.getList("list");
        list.get(0);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testMap_unlock_throwsIllegalMonitorStateException() {
        IMap<Object, Object> map = client.getMap("map");
        map.unlock("foo");
    }
}
