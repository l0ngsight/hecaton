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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapStore;
import uk.org.hecaton.map.MapStoreAdapter;
import uk.org.hecaton.map.impl.mapstore.AbstractMapStoreTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static uk.org.hecaton.internal.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapLoaderExceptionHandlingTest extends AbstractMapStoreTest {

    private static final String mapName = randomMapName();
    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance client;
    private ExceptionalMapStore mapStore;

    @Before
    public void setup() {
        mapStore = new ExceptionalMapStore();
        Config config = createNewConfig(mapName, mapStore);
        hecatonFactory.newHecatonInstance(config);
        client = hecatonFactory.newHecatonClient();
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void configureMapStore() {
        mapStore.setLoadAllKeysThrows(false);
    }

    @Test
    public void test_initial_map_load_propagates_exception_to_client() {
        final IMap<Integer, Integer> map = client.getMap(mapName);

        assertTrueEventually(() -> {
            Exception exception = null;
            try {
                map.get(1);
            } catch (Exception e) {
                exception = e;
            }
            assertNotNull("Exception not propagated to client", exception);
            assertEquals(ClassCastException.class, exception.getClass());
        });
    }

    @Test
    public void testClientGetsException_whenLoadAllKeysThrowsOne() {
        mapStore.setLoadAllKeysThrows(true);

        IMap<Integer, Integer> map = client.getMap(mapName);

        Exception exception = null;
        try {
            map.get(1);
        } catch (Exception e) {
            exception = e;
        }

        assertNotNull("Exception not propagated to client", exception);
        assertEquals(IllegalStateException.class, exception.getClass());
    }

    private Config createNewConfig(String mapName, MapStore store) {
        return newConfig(mapName, store, 0);
    }

    private static class ExceptionalMapStore extends MapStoreAdapter {

        private boolean loadAllKeysThrows = false;

        @Override
        public Set loadAllKeys() {
            checkState(!loadAllKeysThrows, getClass().getName());

            final HashSet<Integer> integers = new HashSet<>();
            for (int i = 0; i < 1000; i++) {
                integers.add(i);
            }

            return integers;
        }

        @Override
        public Map loadAll(Collection keys) {
            throw new ClassCastException("ExceptionalMapStore.loadAll");
        }

        public void setLoadAllKeysThrows(boolean loadAllKeysThrows) {
            this.loadAllKeysThrows = loadAllKeysThrows;
        }
    }
}
