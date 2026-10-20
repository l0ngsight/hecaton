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
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.PartitionContainer;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.config.MaxSizePolicy.PER_NODE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({NightlyTest.class})
public class MapContainerCreationUponDestroyStressTest extends HecatonTestSupport {

    @Parameterized.Parameter(0)
    public boolean forceOffload;

    @Parameterized.Parameters(name = "forceOffload: {0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {false},
                {true}
        });
    }

    private static final int PARTITION_COUNT = 271;

    /**
     * Tests newly created recordStores after imap#destroy references same MapContainer instance.
     * <p>
     * Normally, we remove mapContainer in map#destroy operation. If we call map#put after map#destroy, a new mapContainer
     * instance is created and all newly created recordStores references that new mapContainer instance. That referenced
     * mapContainer instance should be the new-mapContainer-instance because we previously called map#destroy and removed
     * old mapContainer instance.
     * <p>
     * This test trying to be sure that after map#destroy those all newly created recordStore instances references same
     * single mapContainer instance by stressing mapContainer creation process with subsequent map#put and map#destroy operations.
     */
    @Test
    public void testNewRecordStores_sharesSameMapContainerInstance_afterMapDestroy() throws Exception {
        String mapName = "map";
        final IMap<Long, Long> map = getIMap(mapName);
        final AtomicBoolean stop = new AtomicBoolean();

        Thread putThread = new Thread(() -> {
            while (!stop.get()) {
                map.put(System.currentTimeMillis(), System.currentTimeMillis());
            }
        });

        Thread destroyThread = new Thread(() -> {
            while (!stop.get()) {
                map.destroy();
                sleepAtLeastMillis(100);
            }
        });

        putThread.start();
        destroyThread.start();

        sleepSeconds(15);
        stop.set(true);

        putThread.join();
        destroyThread.join();

        // make a final map#put to create recordStores
        for (long i = 0; i < PARTITION_COUNT; i++) {
            map.put(i, i);
        }

        assertRecordStoresSharesSameMapContainerInstance(map);
    }

    private void assertRecordStoresSharesSameMapContainerInstance(IMap<Long, Long> map) {
        String mapName = map.getName();

        MapContainer expectedMapContainer = getMapContainer(map);

        for (int i = 0; i < PARTITION_COUNT; i++) {
            PartitionContainer partitionContainer = getMapServiceContext(map).getPartitionContainer(i);
            RecordStore recordStore = partitionContainer.getMaps().get(mapName);
            if (recordStore == null) {
                continue;
            }
            assertEquals(expectedMapContainer, recordStore.getMapContainer());
        }
    }

    private IMap<Long, Long> getIMap(String mapName) {
        Config config = new Config();
        config.setProperty(MapServiceContext.FORCE_OFFLOAD_ALL_OPERATIONS.getName(),
                String.valueOf(forceOffload));

        MapConfig mapConfig = config.getMapConfig(mapName);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setSize(10000).setMaxSizePolicy(PER_NODE);
        HecatonInstance node = createHecatonInstance(config);

        return node.getMap(mapName);
    }

    private MapContainer getMapContainer(IMap map) {
        MapServiceContext mapServiceContext = getMapServiceContext(map);
        return mapServiceContext.getMapContainers().get(map.getName());
    }

    private MapServiceContext getMapServiceContext(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        RemoteService service = mapProxy.getService();
        MapService mapService = (MapService) service;
        return mapService.getMapServiceContext();
    }
}
