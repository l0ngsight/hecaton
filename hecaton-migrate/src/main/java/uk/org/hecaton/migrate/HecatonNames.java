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

package uk.org.hecaton.migrate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The renames Hecaton applied to Hazelcast 5.7 (spec §5), for use by the migration tools.
 * <p>
 * Java names follow fixed rules: the root package {@code com.hazelcast} becomes
 * {@code uk.org.hecaton} and "Hazelcast" becomes "Hecaton" within identifiers. Other names
 * (property keys, config file names) are only renamed when Hecaton itself reads the renamed
 * name: the list in {@code known-names.txt} is taken from the Hecaton sources (see
 * KnownNamesTest), so an application's own names that merely start with "hazelcast" (a map
 * called {@code hazelcast.sessions}, say) are left alone.
 */
public final class HecatonNames {

    /** Hazelcast's root package. */
    public static final String HAZELCAST_PACKAGE = "com.hazelcast";
    /** Hecaton's root package. */
    public static final String HECATON_PACKAGE = "uk.org.hecaton";

    private static final String HAZELCAST = "hazelcast";
    private static final String HECATON = "hecaton";

    private static final Set<String> EXACT = new HashSet<>();
    private static final List<String> PREFIXES = new ArrayList<>();

    static {
        try (InputStream in = HecatonNames.class.getResourceAsStream("known-names.txt")) {
            if (in == null) {
                throw new IllegalStateException("known-names.txt is missing");
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.endsWith(".") || line.endsWith("-")) {
                    PREFIXES.add(line);
                } else {
                    EXACT.add(line);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private HecatonNames() {
    }

    /**
     * Renames "Hazelcast" within a Java identifier or name, keeping its case style:
     * {@code HazelcastInstance} → {@code HecatonInstance}, {@code HAZELCAST_X} →
     * {@code HECATON_X}, {@code hazelcastInstance} → {@code hecatonInstance}.
     */
    public static String renameIdentifier(String name) {
        return name.replace("HAZELCAST", "HECATON").replace("Hazelcast", "Hecaton").replace(HAZELCAST, HECATON);
    }

    /**
     * Renames a fully qualified Java name ({@code com.hazelcast.core.HazelcastInstance} →
     * {@code uk.org.hecaton.core.HecatonInstance}). Names outside {@code com.hazelcast} are
     * returned unchanged.
     */
    public static String renameJavaName(String fqn) {
        if (!fqn.equals(HAZELCAST_PACKAGE) && !fqn.startsWith(HAZELCAST_PACKAGE + ".")) {
            return fqn;
        }
        return HECATON_PACKAGE + renameIdentifier(fqn.substring(HAZELCAST_PACKAGE.length()));
    }

    /** Whether {@code fqn} is {@code com.hazelcast} or one of its sub-packages or types. */
    public static boolean isHazelcastJavaName(String fqn) {
        return fqn.equals(HAZELCAST_PACKAGE) || fqn.startsWith(HAZELCAST_PACKAGE + ".");
    }

    /**
     * The Hecaton name for a Hazelcast property key or file name, such as
     * {@code hazelcast.logging.type} or {@code hazelcast-client.yaml}; {@code null} if
     * Hecaton does not read the renamed name.
     */
    public static String renameKnownName(String name) {
        if (!name.startsWith(HAZELCAST)) {
            return null;
        }
        String candidate = HECATON + name.substring(HAZELCAST.length());
        if (EXACT.contains(candidate)) {
            return candidate;
        }
        for (String prefix : PREFIXES) {
            if (candidate.length() > prefix.length() && candidate.startsWith(prefix)) {
                return candidate;
            }
        }
        return null;
    }
}
