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
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.impl.EntryCostEstimator;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.StorageImpl;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpirySystem;

/**
 * The entries of one partition of a tiered map: the records are on the heap, keyed by their
 * serialized keys; their values are in the partition's {@link PartitionTiers}.
 */
final class TieredStorage extends StorageImpl<Record> {

    private final PartitionTiers tiers;
    private final SerializationService serializationService;

    TieredStorage(PartitionTiers tiers, ExpirySystem expirySystem, SerializationService serializationService) {
        super(InMemoryFormat.BINARY, expirySystem, serializationService);
        this.tiers = tiers;
        this.serializationService = serializationService;
        setEntryCostEstimator(new MemoryTierCostEstimator());
    }

    PartitionTiers tiers() {
        return tiers;
    }

    @Override
    public void put(Data key, Record record) {
        TieredRecord tiered = record instanceof TieredRecord own ? own : TieredRecord.copyOf(record);
        Record previous = get(key);
        if (previous != null && previous != tiered) {
            release(previous);
        }
        if (tiered.tiers == null) {
            tiers.attach(tiered);
        }
        super.put(key, tiered);
    }

    @Override
    public Record updateRecordValue(Data key, Record record, Object value) {
        record.setValue(serializationService.toData(value));
        return record;
    }

    @Override
    public void removeRecord(Data dataKey, Record record) {
        super.removeRecord(dataKey, record);
        release(record);
    }

    @Override
    public void clear() {
        tiers.clear();
        super.clear();
    }

    @Override
    public void destroy() {
        clear();
    }

    private void release(Record record) {
        if (record instanceof TieredRecord tiered && tiered.tiers != null) {
            tiers.detach(tiered);
        }
    }

    /** Reports the native memory of the partition's memory tier as the owned entry cost. */
    private final class MemoryTierCostEstimator implements EntryCostEstimator<Data, Record> {

        @Override
        public long getEstimate() {
            return tiers.memoryBytes();
        }

        @Override
        public void adjustEstimateBy(long adjustment) {
            // the tiers keep their own count
        }

        @Override
        public long calculateValueCost(Record record) {
            return record.getCost();
        }

        @Override
        public long calculateEntryCost(Data key, Record record) {
            return record.getCost();
        }

        @Override
        public void reset() {
            // the tiers keep their own count
        }
    }
}
