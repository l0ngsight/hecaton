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
package uk.org.hecaton.persistence.internal.store;

/**
 * Settings of {@link PartitionLog}s.
 *
 * @param maxChunkSize           size at which the active chunk is closed and a new one started
 * @param compactionGarbageRatio share of garbage in the closed chunks above which they are compacted
 */
public record LogSettings(long maxChunkSize, double compactionGarbageRatio) {

    /** 4 MB chunks, compacted when half of them is garbage. */
    public static final LogSettings DEFAULT = new LogSettings(1 << 22, 0.5);

    public LogSettings {
        if (maxChunkSize <= ChunkFormat.HEADER_SIZE) {
            throw new IllegalArgumentException("maxChunkSize too small: " + maxChunkSize);
        }
        if (!(compactionGarbageRatio > 0 && compactionGarbageRatio < 1)) {
            throw new IllegalArgumentException("compactionGarbageRatio must be in (0, 1): " + compactionGarbageRatio);
        }
    }
}
