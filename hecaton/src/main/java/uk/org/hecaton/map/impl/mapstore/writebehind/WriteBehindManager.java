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

package uk.org.hecaton.map.impl.mapstore.writebehind;

import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.PartitionContainer;
import uk.org.hecaton.map.impl.mapstore.MapDataStore;
import uk.org.hecaton.map.impl.mapstore.MapDataStores;
import uk.org.hecaton.map.impl.mapstore.MapStoreContext;
import uk.org.hecaton.map.impl.mapstore.MapStoreManager;
import uk.org.hecaton.map.impl.mapstore.writebehind.entry.DelayedEntry;
import uk.org.hecaton.map.impl.recordstore.RecordStore;

import static uk.org.hecaton.map.impl.mapstore.writebehind.WriteBehindProcessors.createWriteBehindProcessor;

/**
 * Write behind map store manager.
 */
public class WriteBehindManager implements MapStoreManager {

    private final WriteBehindProcessor writeBehindProcessor;
    private final StoreWorker storeWorker;
    private final MapStoreContext mapStoreContext;

    public WriteBehindManager(MapStoreContext mapStoreContext) {
        this.mapStoreContext = mapStoreContext;
        this.writeBehindProcessor = newWriteBehindProcessor(mapStoreContext);
        this.storeWorker = new StoreWorker(mapStoreContext, writeBehindProcessor);
    }

    @Override
    public void start() {
        storeWorker.start();
    }

    @Override
    public void stop() {
        storeWorker.stop();
    }

    //todo get this via constructor function.
    @Override
    public MapDataStore getMapDataStore(String mapName, int partitionId) {
        return MapDataStores.createWriteBehindStore(mapStoreContext, partitionId, writeBehindProcessor);
    }

    private WriteBehindProcessor newWriteBehindProcessor(final MapStoreContext mapStoreContext) {
        WriteBehindProcessor writeBehindProcessor = createWriteBehindProcessor(mapStoreContext);
        StoreListener<DelayedEntry> storeListener = new InternalStoreListener(mapStoreContext);
        writeBehindProcessor.addStoreListener(storeListener);
        return writeBehindProcessor;
    }

    /**
     * Store listener which is responsible for
     * {@link uk.org.hecaton.map.impl.mapstore.writebehind.WriteBehindStore#stagingArea} cleaning.
     */
    private static class InternalStoreListener implements StoreListener<DelayedEntry> {

        private final MapStoreContext mapStoreContext;

        InternalStoreListener(MapStoreContext mapStoreContext) {
            this.mapStoreContext = mapStoreContext;
        }

        @Override
        public void beforeStore(StoreEvent<DelayedEntry> storeEvent) {
        }

        /**
         * Here we are cleaning staging area upon a store operation.
         */
        @Override
        public void afterStore(StoreEvent<DelayedEntry> storeEvent) {
            DelayedEntry delayedEntry = storeEvent.getSource();
            int partitionId = delayedEntry.getPartitionId();
            WriteBehindStore writeBehindStore = getWriteBehindStoreOrNull(partitionId);
            if (writeBehindStore == null) {
                return;
            }

            writeBehindStore.removeFromStagingArea(delayedEntry);
        }

        private WriteBehindStore getWriteBehindStoreOrNull(int partitionId) {
            MapStoreContext mapStoreContext = this.mapStoreContext;
            MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
            RecordStore recordStore = partitionContainer.getExistingRecordStore(mapStoreContext.getMapName());
            if (recordStore == null) {
                return null;
            }
            return (WriteBehindStore) recordStore.getMapDataStore();
        }

    }
}
