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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleEvent.LifecycleState;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.internal.jmx.ManagementService;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

public class LifecycleServiceImpl implements LifecycleService {

    private final HecatonInstanceImpl instance;
    private final ConcurrentMap<UUID, LifecycleListener> lifecycleListeners = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();

    public LifecycleServiceImpl(HecatonInstanceImpl instance) {
        this.instance = instance;
    }

    private ILogger getLogger() {
        return instance.node.getLogger(LifecycleService.class.getName());
    }

    @Nonnull
    @Override
    public UUID addLifecycleListener(@Nonnull LifecycleListener lifecycleListener) {
        checkNotNull(lifecycleListener, "lifecycleListener must not be null");
        final UUID id = UuidUtil.newUnsecureUUID();
        lifecycleListeners.put(id, lifecycleListener);
        return id;
    }

    @Override
    public boolean removeLifecycleListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId must not be null");
        return lifecycleListeners.remove(registrationId) != null;
    }

    public void fireLifecycleEvent(LifecycleState lifecycleState) {
        fireLifecycleEvent(new LifecycleEvent(lifecycleState));
    }

    public void fireLifecycleEvent(LifecycleEvent lifecycleEvent) {
        getLogger().info(instance.node.getThisAddress() + " is " + lifecycleEvent.getState());
        for (LifecycleListener lifecycleListener : lifecycleListeners.values()) {
            lifecycleListener.stateChanged(lifecycleEvent);
        }
    }

    @Override
    public boolean isRunning() {
        //no synchronization needed since getState() is atomic.
        return instance.node.isRunning();
    }

    @Override
    public void shutdown() {
        shutdown(false);
    }

    @Override
    public void terminate() {
        shutdown(true);
    }

    private void shutdown(boolean terminate) {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            ManagementService managementService = instance.managementService;
            if (managementService != null) {
                managementService.destroy();
            }
            final Node node = instance.node;
            if (node != null) {
                node.shutdown(terminate);
            }
            HecatonInstanceFactory.remove(instance);
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void runUnderLifecycleLock(final Runnable runnable) {
        synchronized (lifecycleLock) {
            runnable.run();
        }
    }
}
