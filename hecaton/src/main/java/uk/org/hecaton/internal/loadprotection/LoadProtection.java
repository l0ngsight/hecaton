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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.LoadProtectionConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.internal.metrics.Probe;
import uk.org.hecaton.internal.util.counters.MwCounter;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.spi.properties.HecatonProperties;

import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.internal.metrics.ProbeLevel.MANDATORY;
import static uk.org.hecaton.internal.util.counters.MwCounter.newMwCounter;

/**
 * A member's load protection (F23): whether it is on, its configuration, and the parts that are
 * shared by the subsystems it changes. One instance per member, created with the node.
 */
public final class LoadProtection {

    /** The prefix of the load protection metrics. */
    public static final String METRICS_PREFIX = "loadProtection";

    @Probe(name = "immutableRejections", level = MANDATORY)
    final MwCounter immutableRejections = newMwCounter();
    @Probe(name = "immutableMergeConflicts", level = MANDATORY)
    final MwCounter immutableMergeConflicts = newMwCounter();

    private final boolean enabled;
    private final LoadProtectionConfig config;
    private final ILogger logger;

    public LoadProtection(Config config, HecatonProperties properties, ILogger logger) {
        this.config = new LoadProtectionConfig(config.getLoadProtectionConfig());
        String override = properties.getString(ClusterProperty.LOAD_PROTECTION_ENABLED);
        this.enabled = override == null ? this.config.isEnabled() : Boolean.parseBoolean(override.trim());
        this.logger = logger;
        if (enabled) {
            logger.info("Load protection is on: " + this.config);
        } else {
            warnAboutIgnoredSettings(config);
        }
    }

    /** Whether load protection is on. */
    public boolean isEnabled() {
        return enabled;
    }

    /** The configuration, as it was when the member started. */
    public LoadProtectionConfig getConfig() {
        return config;
    }

    /** Whether the map is immutable: load protection is on and the map is configured immutable. */
    public boolean isImmutable(MapConfig mapConfig) {
        return enabled && mapConfig.isImmutable();
    }

    /** The number of shards of the queue: its shard count with load protection on, 1 otherwise. */
    public int shardCount(QueueConfig queueConfig) {
        return enabled ? queueConfig.getShardCount() : 1;
    }

    /** Counts an operation refused because it would change an immutable map. */
    public void onImmutableRejection() {
        immutableRejections.inc();
    }

    /** Counts a merge that kept an immutable map's value although the merging value differed. */
    public void onImmutableMergeConflict() {
        immutableMergeConflicts.inc();
    }

    public ILogger getLogger() {
        return logger;
    }

    private void warnAboutIgnoredSettings(Config config) {
        List<String> ignored = new ArrayList<>();
        config.getMapConfigs().forEach((name, mapConfig) -> {
            if (mapConfig.isImmutable()) {
                ignored.add("map " + name + " (immutable)");
            }
        });
        config.getQueueConfigs().forEach((name, queueConfig) -> {
            if (queueConfig.getShardCount() > 1) {
                ignored.add("queue " + name + " (" + queueConfig.getShardCount() + " shards)");
            }
        });
        if (!ignored.isEmpty()) {
            logger.warning("Load protection is off, so these settings are ignored: " + String.join(", ", ignored)
                    + ". Turn it on with Config#getLoadProtectionConfig().setEnabled(true).");
        }
    }
}
