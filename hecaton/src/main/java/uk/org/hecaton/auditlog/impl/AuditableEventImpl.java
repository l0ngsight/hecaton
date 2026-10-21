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
import uk.org.hecaton.auditlog.EventBuilder;
import uk.org.hecaton.auditlog.Level;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An audit log event, and the builder that creates and logs it.
 */
public final class AuditableEventImpl implements AuditableEvent {

    private final String typeId;
    private final String message;
    private final Map<String, Object> parameters;
    private final Level level;
    private final Throwable cause;
    private final long timestamp;

    AuditableEventImpl(String typeId, String message, Map<String, Object> parameters, Level level, Throwable cause,
                       long timestamp) {
        this.typeId = typeId;
        this.message = message;
        this.parameters = parameters;
        this.level = level;
        this.cause = cause;
        this.timestamp = timestamp;
    }

    @Override
    public String typeId() {
        return typeId;
    }

    @Override
    public String message() {
        return message;
    }

    @Override
    public Map<String, Object> parameters() {
        return parameters;
    }

    @Override
    public Level level() {
        return level;
    }

    @Override
    public Throwable cause() {
        return cause;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "[" + typeId + "] " + message + (parameters.isEmpty() ? "" : " " + parameters);
    }

    /** Builds an event; {@link #log()} hands it to the audit log. */
    public static final class Builder implements EventBuilder<Builder> {

        private final String typeId;
        private final Consumer<AuditableEvent> sink;
        private final Map<String, Object> parameters = new LinkedHashMap<>();
        private String message;
        private Level level = Level.INFO;
        private Throwable cause;
        private long timestamp;

        Builder(String typeId, Consumer<AuditableEvent> sink) {
            this.typeId = typeId;
            this.sink = sink;
        }

        @Override
        public Builder message(String message) {
            this.message = message;
            return this;
        }

        @Override
        public Builder parameters(Map<String, Object> parameters) {
            this.parameters.clear();
            this.parameters.putAll(parameters);
            return this;
        }

        @Override
        public Builder addParameter(String key, Object value) {
            parameters.put(key, value);
            return this;
        }

        @Override
        public Builder level(Level level) {
            this.level = level;
            return this;
        }

        @Override
        public Builder cause(Throwable throwable) {
            this.cause = throwable;
            return this;
        }

        @Override
        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        @Override
        public AuditableEvent build() {
            return new AuditableEventImpl(typeId, message, Collections.unmodifiableMap(new LinkedHashMap<>(parameters)),
                    level, cause, timestamp == 0 ? System.currentTimeMillis() : timestamp);
        }

        @Override
        public void log() {
            sink.accept(build());
        }
    }
}
