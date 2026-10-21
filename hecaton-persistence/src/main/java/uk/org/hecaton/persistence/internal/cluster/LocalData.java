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
package uk.org.hecaton.persistence.internal.cluster;

/**
 * The persisted data of a member's partitions, as recovery needs it. Methods taking a partition
 * run on the partition's thread.
 */
public interface LocalData {

    /** Whether the partition has persisted data. */
    boolean hasPartition(int partitionId);

    /**
     * Loads the partition's persisted data into the record stores.
     *
     * @param owner whether the member owns the partition (and builds its indexes)
     */
    void load(int partitionId, boolean owner);

    /** Deletes the partition's persisted data. */
    void delete(int partitionId);

    /** Deletes all persisted data; the partitions' logs must not be in use. */
    void deleteAll();

    /**
     * Makes the backups of an owned partition out of date, so the owner sends them again (anti-entropy).
     */
    void markBackupsStale(int partitionId);
}
