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
package uk.org.hecaton.security.impl;

import uk.org.hecaton.config.LoginModuleConfig;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The JAAS configuration of a realm: its login modules, in order, for any application name.
 */
final class RealmLoginConfiguration extends Configuration {

    private final AppConfigurationEntry[] entries;

    RealmLoginConfiguration(LoginModuleConfig[] loginModules) {
        entries = new AppConfigurationEntry[loginModules.length];
        for (int i = 0; i < loginModules.length; i++) {
            LoginModuleConfig module = loginModules[i];
            entries[i] = new AppConfigurationEntry(module.getClassName(), flag(module.getUsage()),
                    options(module.getProperties()));
        }
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return entries.clone();
    }

    private static LoginModuleControlFlag flag(LoginModuleConfig.LoginModuleUsage usage) {
        if (usage == null) {
            return LoginModuleControlFlag.REQUIRED;
        }
        return switch (usage) {
            case REQUISITE -> LoginModuleControlFlag.REQUISITE;
            case SUFFICIENT -> LoginModuleControlFlag.SUFFICIENT;
            case OPTIONAL -> LoginModuleControlFlag.OPTIONAL;
            default -> LoginModuleControlFlag.REQUIRED;
        };
    }

    private static Map<String, Object> options(Properties properties) {
        Map<String, Object> options = new HashMap<>();
        if (properties != null) {
            properties.stringPropertyNames().forEach(name -> options.put(name, properties.getProperty(name)));
        }
        return options;
    }
}
