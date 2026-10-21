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

package uk.org.hecaton.internal.util;

/**
 * The features that Hazelcast reserves for its Enterprise edition and Hecaton provides itself
 * (spec §6), with their spec IDs and the milestone that delivers them (implementation plan).
 * <p>
 * Where Hazelcast checked for the Enterprise edition, Hecaton checks {@link #isAvailable()}:
 * configuring a feature that is not available yet fails with {@link #notAvailableMessage()}.
 * A feature becomes available when its implementation lands.
 */
public enum HecatonFeature {

    /** F1. */
    TLS("F1", "TLS/SSL", "M3", null, true),
    /** F2, F3: authentication and authorization ({@code SecurityConfig}). */
    SECURITY("F2/F3", "Security (authentication and authorization)", "M3", null, true),
    /** F2 (P1): LDAP and Kerberos realms. */
    LDAP_KERBEROS_AUTHENTICATION("F2", "LDAP and Kerberos authentication", "M9", null, true),
    /** F4. */
    AUDIT_LOG("F4", "Audit log", "M9", null, true),
    /** F5. */
    SOCKET_INTERCEPTOR("F5", "Socket interceptor", "M9", null, true),
    /** F6: not planned, TLS replaces it. */
    SYMMETRIC_ENCRYPTION("F6", "Symmetric encryption", null, "use TLS/SSL (F1) instead"),
    /** F7. */
    CP_SUBSYSTEM("F7", "CP Subsystem", "M4", null, true),
    /** F8. */
    CP_MAP("F8", "CPMap", "M4", null, true),
    /** F10. */
    PERSISTENCE("F10", "Persistence (Hot Restart)", "M6", null, false, "persistence"),
    /** F11. */
    HIGH_DENSITY_MEMORY("F11", "High-Density memory (NATIVE in-memory format)", "M5", null, false, "hd"),
    /** F12. */
    TIERED_STORE("F12", "Tiered Storage", "M10"),
    /** F9. */
    CP_PERSISTENCE("F9", "CP Subsystem persistence", "M9"),
    /** F13. */
    WAN_REPLICATION("F13", "WAN replication", "M7", null, true),
    /** F13: synchronization and consistency checks. */
    WAN_SYNC("F13", "WAN synchronization", "M9", null, true),
    /** F16. */
    DYNAMIC_CONFIGURATION_PERSISTENCE("F16", "Dynamic configuration persistence", "M9", null, true),
    /** F16. */
    CONFIGURATION_RELOAD("F16", "Configuration reload and update", "M9", null, true),
    /** F17. */
    USER_CODE_NAMESPACES("F17", "User Code Namespaces", "M9"),
    /** F18. */
    JET_LOSSLESS_RESTART("F18", "Jet lossless restart", "M10"),
    /** F18. */
    JET_ISOLATED_JOBS("F18", "Jet isolated jobs", "M10"),
    /** F18. */
    JET_JOB_UPGRADE("F18", "Jet job upgrade and exported snapshots", "M10"),
    /** F19. */
    SQL_CATALOG_PERSISTENCE("F19", "SQL catalog persistence", "M10"),
    /** F21: Hecaton has no licenses. */
    LICENSE("F21", "License management", null, "Hecaton needs no license; license keys are ignored"),
    /** Not in the spec's scope (yet). */
    THREAD_PER_CORE("-", "Thread-per-core (TPC)", null, null),
    /** Not in the spec's scope (yet). */
    MULTI_MEMBER_ROUTING("-", "Client MULTI_MEMBER routing", null, "use ALL_MEMBERS or SINGLE_MEMBER routing");

    private final String id;
    private final String displayName;
    private final String milestone;
    private final String alternative;
    private final boolean available;
    private final String module;

    HecatonFeature(String id, String displayName, String milestone) {
        this(id, displayName, milestone, null, false);
    }

    HecatonFeature(String id, String displayName, String milestone, String alternative) {
        this(id, displayName, milestone, alternative, false);
    }

    HecatonFeature(String id, String displayName, String milestone, String alternative, boolean available) {
        this(id, displayName, milestone, alternative, available, null);
    }

    /**
     * @param module the member module that provides the feature ({@code uk.org.hecaton:hecaton-<module>}),
     *               or null if core does
     */
    HecatonFeature(String id, String displayName, String milestone, String alternative, boolean available,
                   String module) {
        this.id = id;
        this.displayName = displayName;
        this.milestone = milestone;
        this.alternative = alternative;
        this.available = available;
        this.module = module;
    }

    /**
     * Whether Hecaton implements the feature. A feature provided by a member module is available
     * when the module is on the classpath.
     */
    public boolean isAvailable() {
        return available || (module != null && InstalledMemberModules.contains(module));
    }

    /** The spec ID, such as {@code F1}. */
    public String id() {
        return id;
    }

    /** Why the feature cannot be used: not implemented yet (and when it is planned), or not supported. */
    public String notAvailableMessage() {
        String name = "-".equals(id) ? displayName : displayName + " (" + id + ")";
        if (module != null) {
            return name + " requires uk.org.hecaton:hecaton-" + module + " on the member's classpath.";
        }
        if (milestone == null) {
            return name + " is not supported by Hecaton" + (alternative == null ? "." : "; " + alternative + ".");
        }
        return name + " is not available in Hecaton yet; it is planned for milestone " + milestone
                + " (see the implementation plan).";
    }
}
