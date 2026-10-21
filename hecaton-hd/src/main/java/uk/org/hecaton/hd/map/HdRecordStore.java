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
package uk.org.hecaton.hd.map;

import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapKeyLoader;
import uk.org.hecaton.map.impl.record.RecordFactory;
import uk.org.hecaton.map.impl.recordstore.DefaultRecordStore;
import uk.org.hecaton.map.impl.recordstore.Storage;

/**
 * The record store of a partition of a {@code NATIVE} map: the default record store logic over a
 * {@link NativeStorage}.
 */
public class HdRecordStore extends DefaultRecordStore {

    private final HdMemoryManager memory;

    public HdRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader, ILogger logger,
                         HdMemoryManager memory) {
        super(mapContainer, partitionId, keyLoader, logger);
        this.memory = memory;
    }

    @Override
    protected RecordFactory createRecordFactory() {
        return (key, value) -> new NativeRecord(serializationService.toData(value));
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return new NativeStorage(memory, getExpirySystem(), serializationService);
    }
}
