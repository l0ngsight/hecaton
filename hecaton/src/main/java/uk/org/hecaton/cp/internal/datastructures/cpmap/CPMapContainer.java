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
package uk.org.hecaton.cp.internal.datastructures.cpmap;

import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.datastructures.spi.atomic.RaftAtomicValue;
import uk.org.hecaton.internal.serialization.Data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * State machine of one {@link uk.org.hecaton.cp.CPMap}. Runs only on the Raft node thread of its
 * group, so it needs no synchronization. Every change carries the time the operation was created,
 * which is part of the replicated log entry, so all replicas record the same timestamps.
 */
public class CPMapContainer extends RaftAtomicValue<Map<Data, CPMapEntry>> {

    private final Map<Data, CPMapEntry> entries;
    private final long maxSizeBytes;
    private long sizeBytes;

    CPMapContainer(CPGroupId groupId, String name, Map<Data, CPMapEntry> entries, long maxSizeBytes) {
        super(groupId, name);
        this.entries = entries == null ? new HashMap<>() : new HashMap<>(entries);
        this.maxSizeBytes = maxSizeBytes;
        for (Map.Entry<Data, CPMapEntry> e : this.entries.entrySet()) {
            sizeBytes += sizeOf(e.getKey(), e.getValue().value());
        }
    }

    /**
     * Returns a copy of the entries, so a snapshot is not changed by later operations.
     */
    @Override
    public Map<Data, CPMapEntry> get() {
        return new HashMap<>(entries);
    }

    public Data get(Data key) {
        CPMapEntry entry = entries.get(key);
        return entry == null ? null : entry.value();
    }

    /**
     * Associates the key with the value and returns the previous value.
     *
     * @throws IllegalStateException if the map would exceed its configured size
     */
    public Data put(Data key, Data value, long timestamp) {
        CPMapEntry old = entries.get(key);
        long newSize = sizeBytes + sizeOf(key, value) - (old == null ? 0 : sizeOf(key, old.value()));
        if (newSize > maxSizeBytes) {
            throw new IllegalStateException("CPMap '" + name() + "' cannot grow to " + newSize
                    + " bytes: its maximum size is " + maxSizeBytes + " bytes (CPMapConfig.maxSizeMb)");
        }
        entries.put(key, new CPMapEntry(value, timestamp));
        sizeBytes = newSize;
        return old == null ? null : old.value();
    }

    public Data remove(Data key) {
        CPMapEntry old = entries.remove(key);
        if (old == null) {
            return null;
        }
        sizeBytes -= sizeOf(key, old.value());
        return old.value();
    }

    public boolean compareAndSet(Data key, Data expected, Data newValue, long timestamp) {
        CPMapEntry current = entries.get(key);
        if (current == null || !current.value().equals(expected)) {
            return false;
        }
        put(key, newValue, timestamp);
        return true;
    }

    /**
     * Removes the entries last written before {@code cutoff}.
     */
    public CPMapPurgeResponseImpl purge(long cutoff) {
        int purged = 0;
        long oldestRemaining = -1;
        Iterator<Map.Entry<Data, CPMapEntry>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Data, CPMapEntry> e = it.next();
            long timestamp = e.getValue().timestamp();
            if (timestamp < cutoff) {
                sizeBytes -= sizeOf(e.getKey(), e.getValue().value());
                it.remove();
                purged++;
            } else if (oldestRemaining == -1 || timestamp < oldestRemaining) {
                oldestRemaining = timestamp;
            }
        }
        return new CPMapPurgeResponseImpl(purged, oldestRemaining);
    }

    public int size() {
        return entries.size();
    }

    public long sizeBytes() {
        return sizeBytes;
    }

    private static long sizeOf(Data key, Data value) {
        return (long) key.totalSize() + value.totalSize();
    }

    @Override
    public String toString() {
        return "CPMapContainer{groupId=" + groupId() + ", name='" + name() + "', size=" + entries.size()
                + ", sizeBytes=" + sizeBytes + '}';
    }
}
