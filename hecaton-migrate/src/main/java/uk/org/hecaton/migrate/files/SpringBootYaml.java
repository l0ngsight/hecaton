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

package uk.org.hecaton.migrate.files;

/**
 * Renames the nested Spring Boot key {@code spring: hazelcast:} to {@code spring: hecaton:}
 * (the flat form {@code spring.hazelcast.config} is a plain text rule).
 */
final class SpringBootYaml {

    private static final String HAZELCAST_KEY = "hazelcast:";

    private SpringBootYaml() {
    }

    static String renameNestedKey(String text) {
        if (!text.contains(HAZELCAST_KEY)) {
            return text;
        }
        String[] lines = text.split("\n", -1);
        int springIndent = -1;
        int childIndent = -1;
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            String body = line.strip();
            if (body.isEmpty() || body.startsWith("#")) {
                continue;
            }
            int indent = line.length() - line.stripLeading().length();
            if (springIndent >= 0 && indent <= springIndent) {
                springIndent = -1;
                childIndent = -1;
            }
            if (springIndent < 0) {
                if (isKey(body, "spring:")) {
                    springIndent = indent;
                }
                continue;
            }
            if (childIndent < 0) {
                childIndent = indent;
            }
            lines[i] = renameChild(line, body, indent, childIndent);
        }
        return String.join("\n", lines);
    }

    private static String renameChild(String line, String body, int indent, int childIndent) {
        if (indent == childIndent && isKey(body, HAZELCAST_KEY)) {
            return line.substring(0, indent) + "hecaton" + line.substring(indent + "hazelcast".length());
        }
        return line;
    }

    private static boolean isKey(String body, String key) {
        return body.equals(key) || body.startsWith(key + " ");
    }
}
