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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.WanCustomPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.MapUtil;
import uk.org.hecaton.wan.WanQueueFullException;
import uk.org.hecaton.wan.impl.WanFullQueuePublisher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapWANExceptionTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance client;
    private HecatonInstance server;

    @Before
    public void setup() {
        server = hecatonFactory.newHecatonInstance(getConfig());
        client = hecatonFactory.newHecatonClient();
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test(expected = WanQueueFullException.class)
    public void testMapPut() {
        IMap<Object, Object> map = client.getMap("wan-exception-client-test-map");
        map.put(1, 1);
    }

    @Test(expected = WanQueueFullException.class)
    public void testMapPutAll() {
        IMap<Object, Object> map = client.getMap("wan-exception-client-test-map");
        Map<Object, Object> inputMap = MapUtil.createHashMap(1);
        inputMap.put(1, 1);
        map.putAll(inputMap);
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("dummyWan");

        wanReplicationConfig.addCustomPublisherConfig(getWanPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("dummyWan");
        wanRef.setMergePolicyClassName(PassThroughMergePolicy.class.getName());

        config.addWanReplicationConfig(wanReplicationConfig);
        config.getMapConfig("default").setWanReplicationRef(wanRef);
        return config;
    }

    private WanCustomPublisherConfig getWanPublisherConfig() {
        WanCustomPublisherConfig pc = new WanCustomPublisherConfig();
        pc.setPublisherId("customPublisherId")
          .setClassName(WanFullQueuePublisher.class.getName());
        return pc;
    }
}
