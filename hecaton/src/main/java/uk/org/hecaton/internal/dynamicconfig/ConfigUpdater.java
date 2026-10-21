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

package uk.org.hecaton.internal.dynamicconfig;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.internal.config.ConfigNamespace;
import uk.org.hecaton.logging.ILogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Configuration reload and update (F16): applies the data structure configurations of a new
 * declarative configuration to the running cluster. Configurations the cluster does not have are
 * added (cluster-wide, as dynamic configurations); configurations that differ from the cluster's
 * are ignored and reported, since a running data structure's configuration cannot change.
 */
class ConfigUpdater {

    private final Config config;
    private final ILogger logger;

    /** @param config the member's configuration, which adds dynamic configurations cluster-wide */
    ConfigUpdater(Config config, ILogger logger) {
        this.config = config;
        this.logger = logger;
    }

    /** Applies {@code newConfig}, or the member's configuration file when it is {@code null}. */
    ConfigUpdateResult update(Config newConfig) {
        Config source = newConfig != null ? newConfig : reloadFile();
        Set<ConfigNamespace> added = new LinkedHashSet<>();
        Set<ConfigNamespace> ignored = new LinkedHashSet<>();
        for (DynamicConfigSection<?> section : DynamicConfigSection.ALL) {
            apply(section, source, added, ignored);
        }
        logger.info("Configuration update: added " + added + ", ignored (changed) " + ignored);
        return new ConfigUpdateResult(added, ignored);
    }

    private <T> void apply(DynamicConfigSection<T> section, Config source, Set<ConfigNamespace> added,
                           Set<ConfigNamespace> ignored) {
        Map<String, T> current = section.configs().apply(config);
        for (Map.Entry<String, T> entry : section.configs().apply(source).entrySet()) {
            T existing = current.get(entry.getKey());
            ConfigNamespace namespace = new ConfigNamespace(section.section(), entry.getKey());
            if (existing == null) {
                section.adder().accept(config, entry.getValue());
                added.add(namespace);
            } else if (!existing.equals(entry.getValue())) {
                ignored.add(namespace);
            }
        }
    }

    private Config reloadFile() {
        File file = config.getConfigurationFile();
        if (file == null) {
            throw new InvalidConfigurationException("The member was not configured from a file, so it cannot reload it");
        }
        try {
            return Config.loadFromFile(file);
        } catch (FileNotFoundException e) {
            throw new InvalidConfigurationException("The configuration file " + file + " does not exist", e);
        }
    }
}
