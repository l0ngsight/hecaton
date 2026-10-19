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

package uk.org.hecaton.client.impl.protocol.codec.holder;

import java.util.Map;
import java.util.Objects;

/**
 * Cloud-provider discovery section (AWS, GCP, Azure, Eureka) of a WAN batch publisher
 * config in the client protocol. Hecaton removed these discovery mechanisms (D9) but keeps
 * the section on the wire, so official Hazelcast 5.x clients can still add WAN configs
 * dynamically: Hecaton sends disabled sections and rejects enabled ones it receives.
 */
public final class RemovedDiscoveryConfigHolder {

    private final String tag;
    private final boolean enabled;
    private final boolean usePublicIp;
    private final Map<String, String> properties;

    public RemovedDiscoveryConfigHolder(String tag, boolean enabled, boolean usePublicIp, Map<String, String> properties) {
        this.tag = tag;
        this.enabled = enabled;
        this.usePublicIp = usePublicIp;
        this.properties = properties;
    }

    /**
     * A disabled section without properties, as Hecaton sends it.
     */
    public static RemovedDiscoveryConfigHolder disabled(String tag) {
        return new RemovedDiscoveryConfigHolder(tag, false, false, Map.of());
    }

    public String getTag() {
        return tag;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isUsePublicIp() {
        return usePublicIp;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RemovedDiscoveryConfigHolder that)) {
            return false;
        }
        return enabled == that.enabled && usePublicIp == that.usePublicIp && Objects.equals(tag, that.tag)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, enabled, usePublicIp, properties);
    }
}
