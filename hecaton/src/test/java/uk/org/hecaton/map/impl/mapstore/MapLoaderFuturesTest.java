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

package uk.org.hecaton.map.impl.mapstore;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.recordstore.DefaultRecordStore;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapLoaderFuturesTest extends HecatonTestSupport {

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void zero_remaining_loading_future_after_multiple_loadAll() {
        String mapName = "load-futures";
        int keyCountToLoad = 10;

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(new SimpleMapLoader(keyCountToLoad, false));

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        HecatonInstance node = createHecatonInstance(config);
        IMap map = node.getMap(mapName);

        for (int i = 0; i < 3; i++) {
            map.loadAll(true);
        }

        assertEquals(keyCountToLoad, map.size());
        assertEquals(0, loadingFutureCount(mapName, node));
    }

    private static int loadingFutureCount(String mapName, HecatonInstance node) {
        int count = 0;
        NodeEngineImpl nodeEngine = getNode(node).getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            RecordStore recordStore = mapServiceContext.getExistingRecordStore(i, mapName);
            if (recordStore != null) {
                count += ((DefaultRecordStore) recordStore).getLoadingFutures().size();
            }
        }
        return count;
    }
}
