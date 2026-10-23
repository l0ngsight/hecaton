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

package uk.org.hecaton.benchmarks.fanout;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Bytes allocated by each live thread, from {@link com.sun.management.ThreadMXBean}: exact, unlike
 * the sampled allocation events of Flight Recorder.
 *
 * @param bytesByThreadName bytes allocated, by thread name
 */
record ThreadAllocations(Map<String, Long> bytesByThreadName) {

    static ThreadAllocations snapshot() {
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        Map<String, Long> bytes = new HashMap<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            long allocated = threads.getThreadAllocatedBytes(thread.threadId());
            if (allocated >= 0) {
                bytes.merge(thread.getName(), allocated, Long::sum);
            }
        }
        return new ThreadAllocations(bytes);
    }

    /** What each thread allocated between this snapshot and {@code later}; new threads count from zero. */
    ThreadAllocations until(ThreadAllocations later) {
        Map<String, Long> delta = new HashMap<>();
        later.bytesByThreadName.forEach((name, bytes) -> {
            long allocated = bytes - bytesByThreadName.getOrDefault(name, 0L);
            if (allocated > 0) {
                delta.put(name, allocated);
            }
        });
        return new ThreadAllocations(delta);
    }
}
