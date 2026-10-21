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
package uk.org.hecaton.hd.memory;

import uk.org.hecaton.internal.memory.DefaultMemoryStats;

/**
 * Memory statistics including the native memory of a {@link HdMemoryManager}.
 */
public final class HdMemoryStats extends DefaultMemoryStats {

    private final HdMemoryManager memoryManager;

    public HdMemoryStats(HdMemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    @Override
    public long getMaxNative() {
        return memoryManager.capacity();
    }

    @Override
    public long getCommittedNative() {
        return memoryManager.committedNative();
    }

    @Override
    public long getUsedNative() {
        return memoryManager.usedNative();
    }

    @Override
    public long getFreeNative() {
        return Math.max(0, memoryManager.capacity() - memoryManager.committedNative() - memoryManager.usedMetadata());
    }

    @Override
    public long getMaxMetadata() {
        return memoryManager.maxMetadata();
    }

    @Override
    public long getUsedMetadata() {
        return memoryManager.usedMetadata();
    }
}
