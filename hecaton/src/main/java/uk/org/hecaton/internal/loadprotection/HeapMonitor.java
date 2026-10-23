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

package uk.org.hecaton.internal.loadprotection;

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The heap occupancy after the last garbage collection, from the collectors' notifications: unlike
 * the heap in use at any moment, it does not count garbage that the next collection frees.
 */
final class HeapMonitor implements NotificationListener {

    private final Set<String> heapPools = new HashSet<>();
    private final long maxHeap = Runtime.getRuntime().maxMemory();
    private volatile long usedAfterLastGc;

    HeapMonitor() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                heapPools.add(pool.getName());
            }
        }
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (collector instanceof NotificationEmitter emitter) {
                emitter.addNotificationListener(this, null, null);
            }
        }
    }

    /** The fraction of the maximum heap in use after the last garbage collection; 0 before the first one. */
    double occupancyAfterGc() {
        return maxHeap <= 0 || maxHeap == Long.MAX_VALUE ? 0 : (double) usedAfterLastGc / maxHeap;
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
        if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            return;
        }
        GarbageCollectionNotificationInfo info =
                GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
        long used = 0;
        for (Map.Entry<String, MemoryUsage> pool : info.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
            if (heapPools.contains(pool.getKey())) {
                used += pool.getValue().getUsed();
            }
        }
        usedAfterLastGc = used;
    }

    void stop() {
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (collector instanceof NotificationEmitter emitter) {
                try {
                    emitter.removeNotificationListener(this);
                } catch (ListenerNotFoundException ignored) {
                    // not registered with this collector
                }
            }
        }
    }
}
