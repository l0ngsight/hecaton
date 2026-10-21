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

package uk.org.hecaton.auditlog.impl;

import uk.org.hecaton.auditlog.AuditableEvent;
import uk.org.hecaton.auditlog.AuditlogService;
import uk.org.hecaton.auditlog.EventBuilder;
import uk.org.hecaton.auditlog.Level;
import uk.org.hecaton.logging.ILogger;

/**
 * Writes audit log events to a logger (by default {@value ILoggerAuditlogServiceFactory#DEFAULT_LOGGER_NAME}),
 * so the member's logging configuration decides where they go.
 */
public class ILoggerAuditlogService implements AuditlogService {

    private final ILogger logger;

    public ILoggerAuditlogService(ILogger logger) {
        this.logger = logger;
    }

    @Override
    public void log(AuditableEvent event) {
        java.util.logging.Level level = toLoggingLevel(event.level());
        if (logger.isLoggable(level)) {
            if (event.cause() == null) {
                logger.log(level, event.toString());
            } else {
                logger.log(level, event.toString(), event.cause());
            }
        }
    }

    @Override
    public void log(String eventTypeId, Level level, String message) {
        eventBuilder(eventTypeId).level(level).message(message).log();
    }

    @Override
    public void log(String eventTypeId, Level level, String message, Throwable thrown) {
        eventBuilder(eventTypeId).level(level).message(message).cause(thrown).log();
    }

    @Override
    public EventBuilder<?> eventBuilder(String typeId) {
        return new AuditableEventImpl.Builder(typeId, this::log);
    }

    private static java.util.logging.Level toLoggingLevel(Level level) {
        return switch (level) {
            case DEBUG -> java.util.logging.Level.FINE;
            case INFO -> java.util.logging.Level.INFO;
            case WARN -> java.util.logging.Level.WARNING;
            case ERROR -> java.util.logging.Level.SEVERE;
        };
    }
}
