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

package uk.org.hecaton.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EntryListenerConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EventListener;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

/**
 * Keep this test serial. It relays on static shared variables.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class EntryListenerConfigTest extends HecatonTestSupport {

    private final String mapName = randomMapName();
    private final EntryListenerConfig listenerConfig = new EntryListenerConfig();
    private HecatonInstance instance;

    @Before
    public void setUp() throws Exception {
        init();
    }

    @After
    public void tearDown() throws Exception {
        init();
    }

    @Test
    public void testMapListenerAddition_withClassName() {
        listenerConfig.setClassName(TestMapListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertListenerRegistration();
    }

    @Test
    public void testMapListenerAddition_withImplementation() {
        listenerConfig.setImplementation(new TestMapListener());
        createInstanceAndInitializeListeners();

        assertListenerRegistration();
    }

    @Test
    public void testHecatonInstanceAwareness_whenMapListenerAdded_withImplementation() {
        listenerConfig.setImplementation(new TestMapListener());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestMapListener.INSTANCE_AWARE);
    }

    @Test
    public void testHecatonInstanceAwareness_whenMapListenerAdded_withClassName() {
        listenerConfig.setClassName(TestMapListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestMapListener.INSTANCE_AWARE);
    }

    @Test
    public void testEntryListenerAddition_withClassName() {
        listenerConfig.setClassName(TestEntryListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertListenerRegistration();
    }

    @Test
    public void testEntryListenerAddition_withImplementation() {
        listenerConfig.setImplementation(new TestEntryListener());
        createInstanceAndInitializeListeners();

        assertListenerRegistration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testListenerAddition_throwsException_withInvalidListener() {
        listenerConfig.setImplementation(new EventListener() {
        });
    }

    @Test
    public void testHecatonInstanceAwareness_whenEntryListenerAdded_withImplementation() {
        listenerConfig.setImplementation(new TestEntryListener());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestEntryListener.INSTANCE_AWARE);
    }

    @Test
    public void testHecatonInstanceAwareness_whenEntryListenerAdded_withClassName() {
        listenerConfig.setClassName(TestEntryListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestEntryListener.INSTANCE_AWARE);
    }

    private void createInstanceAndInitializeListeners() {
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.getEntryListenerConfigs().add(listenerConfig);
        Config config = new Config().addMapConfig(mapConfig);

        instance = createHecatonInstance(config);
        instance.getMap(mapName);
    }

    private void assertListenerRegistration() {
        boolean hasEventRegistration = getEventService().hasEventRegistration(SERVICE_NAME, mapName);
        assertTrue("Listener should be registered", hasEventRegistration);
    }

    private void assertInstanceSet(final AtomicBoolean instanceSet) {
        assertTrueEventually(() -> assertTrue(instanceSet.get()));
    }

    private EventService getEventService() {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        return nodeEngineImpl.getEventService();
    }

    private void init() {
        TestMapListener.INSTANCE_AWARE.set(false);
        TestEntryListener.INSTANCE_AWARE.set(false);
    }
}
