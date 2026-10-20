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

import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.logging.AbstractLogger;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.LogEvent;
import uk.org.hecaton.logging.LogListener;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.logging.LoggerFactory;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.logging.impl.InternalLoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

public class ClientLoggingService implements LoggingService {

    private final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<>(100);

    private final ConstructorFunction<String, ILogger> loggerConstructor = DefaultLogger::new;

    private final LoggerFactory loggerFactory;
    private final BuildInfo buildInfo;
    private final String instanceName;
    private final boolean detailsEnabled;
    private final boolean shouldShutdownLoggingOnHecatonShutdown;
    private volatile String versionMessage;

    public ClientLoggingService(
            String clusterName, String loggingType, BuildInfo buildInfo, String instanceName, boolean detailsEnabled,
            boolean shutdownLoggingEnabled) {
        this.loggerFactory = Logger.newLoggerFactory(loggingType);
        this.buildInfo = buildInfo;
        this.instanceName = instanceName;
        this.detailsEnabled = detailsEnabled;
        this.shouldShutdownLoggingOnHecatonShutdown = shutdownLoggingEnabled;
        updateClusterName(clusterName);
    }

    public void updateClusterName(String clusterName) {
        this.versionMessage = instanceName + " [" + clusterName + "] [" + buildInfo.getProductVersion() + "] ";
    }

    @Override
    public void addLogListener(@Nonnull Level level, @Nonnull LogListener logListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLogListener(@Nonnull LogListener logListener) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public ILogger getLogger(@Nonnull String name) {
        checkNotNull(name, "name must not be null");
        return getOrPutIfAbsent(mapLoggers, name, loggerConstructor);
    }

    @Nonnull
    @Override
    public ILogger getLogger(@Nonnull Class<?> clazz) {
        checkNotNull(clazz, "class must not be null");
        return getOrPutIfAbsent(mapLoggers, clazz.getName(), loggerConstructor);
    }

    @Override
    public void shutdown() {
        if (shouldShutdownLoggingOnHecatonShutdown && loggerFactory instanceof InternalLoggerFactory factory) {
            factory.shutdown();
        }
    }

    private class DefaultLogger extends AbstractLogger {

        final String name;
        final ILogger logger;

        DefaultLogger(String name) {
            this.name = name;
            this.logger = loggerFactory.getLogger(name);
        }

        @Override
        public void log(Level level, String message) {
            log(level, message, null);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            if (logger.isLoggable(level)) {
                String logMessage = detailsEnabled ? versionMessage + message : message;
                logger.log(level, logMessage, thrown);
            }
        }

        @Override
        public void log(LogEvent logEvent) {
            // unused
        }

        @Override
        public Level getLevel() {
            return logger.getLevel();
        }

        @Override
        public boolean isLoggable(Level level) {
            return logger.isLoggable(level);
        }
    }
}
