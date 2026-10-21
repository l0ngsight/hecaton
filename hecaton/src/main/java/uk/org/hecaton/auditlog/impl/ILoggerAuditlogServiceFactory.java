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

import uk.org.hecaton.auditlog.AuditlogService;
import uk.org.hecaton.auditlog.AuditlogServiceFactory;
import uk.org.hecaton.logging.Logger;

import javax.security.auth.callback.CallbackHandler;
import java.util.Properties;

/**
 * The audit log used when {@code auditlog} is enabled without a {@code factory-class-name}: events
 * go to the logger named by the property {@code logger} (default {@value #DEFAULT_LOGGER_NAME}).
 */
public class ILoggerAuditlogServiceFactory implements AuditlogServiceFactory {

    /** The logger audit events are written to. */
    public static final String DEFAULT_LOGGER_NAME = "hecaton.auditlog";

    private String loggerName = DEFAULT_LOGGER_NAME;

    @Override
    public void init(CallbackHandler callbackHandler, Properties properties) {
        if (properties != null) {
            loggerName = properties.getProperty("logger", DEFAULT_LOGGER_NAME);
        }
    }

    @Override
    public AuditlogService createAuditlog() {
        return new ILoggerAuditlogService(Logger.getLogger(loggerName));
    }
}
