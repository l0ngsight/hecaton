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

import java.util.concurrent.atomic.AtomicLong;

/** The native memory a map's memory tier uses on a member, and its capacity. */
final class MemoryTierBudget {

    private final long capacity;
    private final AtomicLong used = new AtomicLong();

    MemoryTierBudget(long capacity) {
        this.capacity = capacity;
    }

    void add(long bytes) {
        used.addAndGet(bytes);
    }

    boolean isOver() {
        return used.get() > capacity;
    }

    long used() {
        return used.get();
    }

    long capacity() {
        return capacity;
    }
}
