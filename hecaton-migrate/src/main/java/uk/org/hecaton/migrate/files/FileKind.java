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

import java.util.Locale;
import java.util.Set;

/** The kinds of file {@link FileMigrator} looks at, decided by file name. */
public enum FileKind {
    /** XML files (other than {@code pom.xml}). */
    XML,
    /** YAML files. */
    YAML,
    /** {@code .properties}, environment files, Dockerfiles, scripts, JSON, Helm templates. */
    OTHER_TEXT,
    /** Java sources and build files: the OpenRewrite recipe migrates these. */
    RECIPE,
    /** Not migrated (documentation, binaries, unknown types). */
    SKIPPED;

    private static final Set<String> OTHER_TEXT_EXTENSIONS = Set.of(
            "properties", "env", "sh", "bash", "zsh", "bat", "cmd", "ps1", "json", "tpl", "conf", "cfg", "ini", "sql");
    private static final Set<String> CONTAINER_AND_ENV_FILES = Set.of("dockerfile", "containerfile", ".env");
    private static final Set<String> RECIPE_EXTENSIONS = Set.of("java", "kt", "groovy", "gradle", "kts");

    public static FileKind of(String fileName) {
        String name = fileName.toLowerCase(Locale.ROOT);
        if (name.equals("pom.xml")) {
            return RECIPE;
        }
        if (isContainerOrEnvFile(name)) {
            return OTHER_TEXT;
        }
        int dot = name.lastIndexOf('.');
        return ofExtension(dot >= 0 ? name.substring(dot + 1) : "");
    }

    private static boolean isContainerOrEnvFile(String name) {
        return CONTAINER_AND_ENV_FILES.contains(name) || name.startsWith("dockerfile.")
                || name.endsWith(".dockerfile") || name.startsWith(".env.");
    }

    private static FileKind ofExtension(String extension) {
        if (extension.equals("xml")) {
            return XML;
        } else if (extension.equals("yaml") || extension.equals("yml")) {
            return YAML;
        } else if (OTHER_TEXT_EXTENSIONS.contains(extension)) {
            return OTHER_TEXT;
        } else if (RECIPE_EXTENSIONS.contains(extension)) {
            return RECIPE;
        }
        return SKIPPED;
    }
}
