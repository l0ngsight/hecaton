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

package uk.org.hecaton.collection.impl.queue;

import uk.org.hecaton.collection.LocalQueueStats;

import java.util.List;
import java.util.function.ToLongFunction;

/** The local statistics of a sharded queue: those of its shards, added up (load protection, F23). */
final class ShardedLocalQueueStats implements LocalQueueStats {

    private final List<LocalQueueStats> shards;

    ShardedLocalQueueStats(List<LocalQueueStats> shards) {
        this.shards = shards;
    }

    @Override
    public long getOwnedItemCount() {
        return sum(LocalQueueStats::getOwnedItemCount);
    }

    @Override
    public long getBackupItemCount() {
        return sum(LocalQueueStats::getBackupItemCount);
    }

    @Override
    public long getMinAge() {
        return shards.stream().mapToLong(LocalQueueStats::getMinAge).min().orElse(0);
    }

    @Override
    public long getMaxAge() {
        return shards.stream().mapToLong(LocalQueueStats::getMaxAge).max().orElse(0);
    }

    @Override
    public long getAverageAge() {
        long items = getOwnedItemCount();
        if (items == 0) {
            return 0;
        }
        long weighted = 0;
        for (LocalQueueStats shard : shards) {
            weighted += shard.getAverageAge() * shard.getOwnedItemCount();
        }
        return weighted / items;
    }

    @Override
    public long getOfferOperationCount() {
        return sum(LocalQueueStats::getOfferOperationCount);
    }

    @Override
    public long getRejectedOfferOperationCount() {
        return sum(LocalQueueStats::getRejectedOfferOperationCount);
    }

    @Override
    public long getPollOperationCount() {
        return sum(LocalQueueStats::getPollOperationCount);
    }

    @Override
    public long getEmptyPollOperationCount() {
        return sum(LocalQueueStats::getEmptyPollOperationCount);
    }

    @Override
    public long getOtherOperationsCount() {
        return sum(LocalQueueStats::getOtherOperationsCount);
    }

    @Override
    public long getEventOperationCount() {
        return sum(LocalQueueStats::getEventOperationCount);
    }

    @Override
    public long getCreationTime() {
        return shards.stream().mapToLong(LocalQueueStats::getCreationTime).min().orElse(0);
    }

    private long sum(ToLongFunction<LocalQueueStats> stat) {
        return shards.stream().mapToLong(stat).sum();
    }
}
