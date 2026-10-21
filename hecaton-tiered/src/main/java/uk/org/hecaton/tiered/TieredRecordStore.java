/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.tiered;

import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapKeyLoader;
import uk.org.hecaton.map.impl.record.RecordFactory;
import uk.org.hecaton.map.impl.recordstore.DefaultRecordStore;
import uk.org.hecaton.map.impl.recordstore.Storage;

/**
 * The record store of a partition of a tiered map: the default record store logic over a
 * {@link TieredStorage}.
 */
public class TieredRecordStore extends DefaultRecordStore {

    private final TieredModule module;

    TieredRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader, ILogger logger,
                      TieredModule module) {
        super(mapContainer, partitionId, keyLoader, logger);
        this.module = module;
    }

    @Override
    protected RecordFactory createRecordFactory() {
        return (key, value) -> new TieredRecord(serializationService.toData(value));
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return new TieredStorage(module.tiers(mapContainer, partitionId), getExpirySystem(), serializationService);
    }
}
