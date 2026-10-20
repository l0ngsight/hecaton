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

package uk.org.hecaton.internal.ascii.rest;

/**
 * Maps the REST paths of Hazelcast ({@code /hazelcast/...}) to the Hecaton ones
 * ({@code /hecaton/...}), so health checks, probes and scripts written for Hazelcast keep
 * working after a migration (spec §5.4).
 */
public final class RestPathAliases {

    static final String LEGACY_PREFIX = "/hazelcast/";
    static final String PREFIX = "/hecaton/";

    private RestPathAliases() {
    }

    /**
     * Returns the Hecaton path for a request path: {@code /hazelcast/...} becomes
     * {@code /hecaton/...}; any other path is returned unchanged.
     */
    public static String canonical(String path) {
        return path != null && path.startsWith(LEGACY_PREFIX)
                ? PREFIX + path.substring(LEGACY_PREFIX.length())
                : path;
    }
}
