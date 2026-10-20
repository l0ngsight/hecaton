/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.test;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.core.LifecycleService;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.MERGED;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.MERGE_FAILED;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.MERGING;

/**
 * Protects against in-progress merging.
 */
class MergeBarrier {

    private final AtomicInteger mergedInProgress = new AtomicInteger();
    private final Map<HecatonInstance, UUID> registrations = new HashMap<>();

    MergeBarrier(HecatonInstance[] instances) {
        MergeCountingListener mergeCountingListener = new MergeCountingListener();

        for (HecatonInstance instance : instances) {
            LifecycleService lifecycleService = instance.getLifecycleService();
            if (lifecycleService.isRunning()) {
                UUID registration = lifecycleService.addLifecycleListener(mergeCountingListener);
                registrations.put(instance, registration);
            }
        }
    }

    /**
     * Await until there is no merging in progress.
     */
    void awaitNoMergeInProgressAndClose() {
        while (mergedInProgress.get() != 0) {
            HecatonTestSupport.sleepAtLeastMillis(10);
        }
        close();
    }

    private void close() {
        for (Map.Entry<HecatonInstance, UUID> entry : registrations.entrySet()) {
            HecatonInstance instance = entry.getKey();
            UUID registration = entry.getValue();

            instance.getLifecycleService().removeLifecycleListener(registration);
        }
    }

    private class MergeCountingListener implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            LifecycleEvent.LifecycleState state = event.getState();
            if (state == MERGING) {
                mergedInProgress.incrementAndGet();
            } else if (state == MERGED) {
                mergedInProgress.decrementAndGet();
            } else if (state == MERGE_FAILED) {
                mergedInProgress.decrementAndGet();
            }
        }
    }
}
