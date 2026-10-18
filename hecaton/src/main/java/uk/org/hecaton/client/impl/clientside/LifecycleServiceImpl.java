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

package uk.org.hecaton.client.impl.clientside;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.internal.util.executor.PoolExecutorThreadFactory;

import javax.annotation.Nonnull;
import java.util.EventListener;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.STARTED;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.STARTING;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmpty;

/**
 * Default {@link uk.org.hecaton.core.LifecycleService} implementation for the client.
 */
public final class LifecycleServiceImpl implements LifecycleService {

    private static final long TERMINATE_TIMEOUT_SECONDS = 30;

    private final HecatonClientInstanceImpl client;
    private final ConcurrentMap<UUID, LifecycleListener> lifecycleListeners
            = new ConcurrentHashMap<>();
    private final AtomicBoolean active = new AtomicBoolean();
    private final BuildInfo buildInfo;
    private final ExecutorService executor;

    /** Monitor which ensures that all client components are down when shutdown() is finished. */
    private final Object shutdownMux = new Object();

    public LifecycleServiceImpl(HecatonClientInstanceImpl client) {
        this.client = client;

        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        executor = Executors.newSingleThreadExecutor(
                new PoolExecutorThreadFactory(client.getName() + ".lifecycle-", classLoader));

        final List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
            for (ListenerConfig listenerConfig : listenerConfigs) {
                EventListener implementation = listenerConfig.getImplementation();
                if (implementation == null) {
                    try {
                        implementation = ClassLoaderUtil.newInstance(classLoader, listenerConfig.getClassName());
                    } catch (Exception e) {
                        getLogger().severe(e);
                    }
                }

                if (implementation instanceof LifecycleListener listener) {
                    addLifecycleListener(listener);
                }
            }
        }
        buildInfo = BuildInfoProvider.getBuildInfo();
    }

    private ILogger getLogger() {
        return client.getLoggingService().getLogger(LifecycleService.class);
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

    public void fireLifecycleEvent(LifecycleEvent.LifecycleState lifecycleState) {
        final LifecycleEvent lifecycleEvent = new LifecycleEvent(lifecycleState);
        String revision = buildInfo.getRevision();
        if (isNullOrEmpty(revision)) {
            revision = "";
        } else {
            revision = " - " + revision;
            BuildInfo upstreamInfo = buildInfo.getUpstreamBuildInfo();
            if (upstreamInfo != null) {
                String upstreamRevision = upstreamInfo.getRevision();
                if (!isNullOrEmpty(upstreamRevision)) {
                    revision += ", " + upstreamRevision;
                }
            }
        }

        getLogger().info("HecatonClient " + buildInfo.getVersion() + " ("
                + buildInfo.getBuild() + revision + ") is "
                + lifecycleEvent.getState());

        executor.execute(() -> {
            for (LifecycleListener lifecycleListener : lifecycleListeners.values()) {
                lifecycleListener.stateChanged(lifecycleEvent);
            }
        });
    }

    @Override
    public boolean isRunning() {
        return active.get();
    }

    @Override
    public void shutdown() {
        client.onGracefulShutdown();
        doShutdown();
    }

    @Override
    public void terminate() {
        doShutdown();
    }

    public void start() {
        fireLifecycleEvent(STARTING);
        active.set(true);
        fireLifecycleEvent(STARTED);
    }

    private void doShutdown() {
        synchronized (shutdownMux) {
            if (!active.compareAndSet(true, false)) {
                return;
            }

            fireLifecycleEvent(SHUTTING_DOWN);
            HecatonClient.shutdown(client.getName());
            client.doShutdown();
            fireLifecycleEvent(SHUTDOWN);

            shutdownExecutor();
        }
    }

    private void shutdownExecutor() {
        executor.shutdown();
        try {
            boolean success = executor.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                getLogger().warning("LifecycleService executor awaitTermination could not completed gracefully in "
                        + TERMINATE_TIMEOUT_SECONDS + " seconds. Terminating forcefully.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().warning("LifecycleService executor awaitTermination is interrupted. Terminating forcefully.", e);
            executor.shutdownNow();
        }
    }
}
