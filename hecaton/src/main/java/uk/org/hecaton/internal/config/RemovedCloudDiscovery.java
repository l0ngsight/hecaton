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

package uk.org.hecaton.internal.config;

import uk.org.hecaton.config.InvalidConfigurationException;

import java.util.Set;

/**
 * The cloud-provider discovery configuration that Hazelcast 5.7 supports and Hecaton removed
 * (spec NG6, D9). Config parsers call {@link #check(String)} for every discovery element, so
 * a migrated config that still uses one fails with a message that says what to use instead.
 */
public final class RemovedCloudDiscovery {

    /** Member and client discovery elements: {@code <aws>}, {@code <gcp>}, ... */
    public static final Set<String> TAGS = Set.of("aws", "gcp", "azure", "eureka");

    /** Client element for Hazelcast Cloud (Viridian) discovery. */
    public static final String CLIENT_CLOUD_TAG = "hecaton-cloud";

    static final String MESSAGE = "Cloud-provider discovery is not supported in Hecaton; use tcp-ip, multicast,"
            + " kubernetes or a Discovery SPI implementation";

    private RemovedCloudDiscovery() {
    }

    /**
     * @throws InvalidConfigurationException if {@code tag} is a removed cloud-provider discovery element
     */
    public static void check(String tag) {
        if (TAGS.contains(tag) || CLIENT_CLOUD_TAG.equals(tag)) {
            throw new InvalidConfigurationException("'" + tag + "': " + MESSAGE);
        }
    }
}
