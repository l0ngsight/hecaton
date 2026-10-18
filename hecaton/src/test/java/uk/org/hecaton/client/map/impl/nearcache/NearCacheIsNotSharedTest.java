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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.replicatedmap.ReplicatedMap;

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCacheIsNotSharedTest {

    private TestHecatonFactory factory;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void nearCacheShouldNotBeUsedByDifferentDataStructure() {
        HecatonInstance hz = factory.newHecatonInstance();

        ClientConfig config = new ClientConfig();
        config.addNearCacheConfig(new NearCacheConfig("test"));
        HecatonInstance client = factory.newHecatonClient(config);

        IMap<String, String> map = client.getMap("test");
        ReplicatedMap<String, String> replicatedMap = client.getReplicatedMap("test");

        replicatedMap.put("key", "replicated-map-value");
        map.put("key", "map-value");
        map.get("key");
        assertEquals("replicated-map-value", replicatedMap.get("key"));
    }
}
