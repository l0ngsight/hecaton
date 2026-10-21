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

package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.wan.impl.WanSyncStats;

import java.util.UUID;

/** The statistics of a finished WAN sync of a map on a member. */
record WanSyncStatsImpl(UUID uuid, long durationSecs, int partitionsToSync, int partitionsSynced,
                        int recordsSynced) implements WanSyncStats {

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public long getDurationSecs() {
        return durationSecs;
    }

    @Override
    public int getPartitionsToSync() {
        return partitionsToSync;
    }

    @Override
    public int getPartitionsSynced() {
        return partitionsSynced;
    }

    @Override
    public int getRecordsSynced() {
        return recordsSynced;
    }
}
