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

package uk.org.hecaton.map.impl.wan;

import uk.org.hecaton.config.AbstractWanPublisherConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.WanCustomPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.WanEvent;
import uk.org.hecaton.wan.impl.DelegatingWanScheme;
import uk.org.hecaton.wan.impl.WanDummyPublisher;
import uk.org.hecaton.wan.impl.WanReplicationService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class WANContextInitializationTest extends HecatonTestSupport {

    private TestHecatonInstanceFactory factory;
    private HecatonInstance instance1;
    private HecatonInstance instance2;

    @Before
    public void setUp() {
        factory = createHecatonInstanceFactory(2);
    }

    @After
    public void reset() {
        SlowWanPublisher.invocations.getAndSet(0);
        factory.terminateAll();
    }

    @Override
    protected Config getConfig() {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("slowWan")
                .addCustomPublisherConfig(getPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName("slowWan")
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig("default")
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setWanReplicationRef(wanRef);

        return smallInstanceConfig()
                .addWanReplicationConfig(wanReplicationConfig)
                .addMapConfig(mapConfig);
    }

    protected WanCustomPublisherConfig getPublisherConfig() {
        return new WanCustomPublisherConfig()
                .setPublisherId("dummyPublisherId")
                .setClassName(SlowWanPublisher.class.getName());
    }

    @Test
    public void testInitializationOnNewNodeJoin() {
        HecatonInstance node1 = factory.newHecatonInstance(getConfig());

        IMap<Object, Object> map = node1.getMap("default");
        map.set(1, 1);
        assertEquals("init method of WAN should be called only once", 1, SlowWanPublisher.invocations.get());

        factory.newHecatonInstance(getConfig());
        // wait migrations end
        waitAllForSafeState(factory.getAllHecatonInstances());

        assertEquals("init method of WAN should be called only once", 1, SlowWanPublisher.invocations.get());
        //put new object
        map.set(2, 2);
        assertEquals("init method of WAN should be called twice", 2, SlowWanPublisher.invocations.get());
    }

    @Test
    public void wanReplicationDelegateSupplierLazilyInitialized() {
        initInstances();
        instance1.getMap("default");
        assertEquals(0, SlowWanPublisher.invocations.get());

        for (int i = 0; i < 10; i++) {
            instance1.getMap("default").put(i, i);
        }

        assertEquals(2, SlowWanPublisher.invocations.get());
        //check the queue size
        assertTotalQueueSize(10);
    }


    private void initInstances() {
        instance1 = factory.newHecatonInstance(getConfig());
        instance2 = factory.newHecatonInstance(getConfig());

        assertClusterSizeEventually(2, instance1, instance2);
    }

    private WanDummyPublisher getWanReplicationImpl(HecatonInstance instance) {
        WanReplicationService service = getNodeEngineImpl(instance).getWanReplicationService();
        DelegatingWanScheme delegate = service.getWanReplicationPublishers("slowWan");
        return (SlowWanPublisher) delegate.getPublishers().iterator().next();
    }

    private void assertTotalQueueSize(final int expectedQueueSize) {
        Queue<WanEvent<Object>> eventQueue1 = getWanReplicationImpl(instance1).getEventQueue();
        Queue<WanEvent<Object>> eventQueue2 = getWanReplicationImpl(instance2).getEventQueue();
        assertTrueEventually(() -> assertEquals(expectedQueueSize, eventQueue1.size() + eventQueue2.size()));
    }

    static class SlowWanPublisher extends WanDummyPublisher {
        static AtomicInteger invocations = new AtomicInteger();

        @Override
        public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig wanPublisherConfig) {
            sleepSeconds(2);
            invocations.incrementAndGet();
            super.init(wanReplicationConfig, wanPublisherConfig);
        }
    }
}
