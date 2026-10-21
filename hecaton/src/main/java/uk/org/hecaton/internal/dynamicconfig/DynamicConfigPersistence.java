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

import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DynamicConfigurationConfig;
import uk.org.hecaton.logging.ILogger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Dynamic configuration persistence (F16): writes the data structure configurations added at runtime
 * into the member's declarative configuration file (XML or YAML), so they survive a restart. Before
 * every change the file is copied to the backup directory, which keeps the last {@code backup-count}
 * copies.
 */
class DynamicConfigPersistence {

    private static final String XML_ROOT_END = "</hecaton>";
    private static final int XML_INDENT = 4;
    private static final int YAML_INDENT = 2;
    private static final DateTimeFormatter BACKUP_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS");

    private final Path file;
    private final DynamicConfigurationConfig settings;
    private final Config staticConfig;
    private final ILogger logger;
    private final Set<String> persisted = new HashSet<>();

    DynamicConfigPersistence(File file, DynamicConfigurationConfig settings, Config staticConfig, ILogger logger) {
        this.file = file.toPath();
        this.settings = settings;
        this.staticConfig = staticConfig;
        this.logger = logger;
    }

    /** Writes a configuration added at runtime into the configuration file, unless it is there already. */
    synchronized void persist(Object config) {
        DynamicConfigSection<Object> section = DynamicConfigSection.of(config);
        if (section == null) {
            logger.fine("Configurations of type " + config.getClass().getSimpleName() + " are not persisted");
            return;
        }
        String name = section.nameOf(config);
        // configurations of the file come back as dynamic ones from the other members after a restart
        if (section.configs().apply(staticConfig).containsKey(name) || !persisted.add(section.section() + ":" + name)) {
            return;
        }
        try {
            backup();
            String content = Files.readString(file, StandardCharsets.UTF_8);
            String updated = isXml() ? insertXml(content, section.generate(config, true, XML_INDENT))
                    : mergeYaml(content, section.section().getName(), section.generate(config, false, YAML_INDENT));
            Path temp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(temp, updated, StandardCharsets.UTF_8);
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            logger.info("Persisted the " + section.section().getName() + " configuration " + name + " to " + file);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot persist the configuration " + name + " to " + file, e);
        }
    }

    private boolean isXml() {
        return file.getFileName().toString().endsWith(".xml");
    }

    private static String insertXml(String content, String snippet) {
        int end = content.lastIndexOf(XML_ROOT_END);
        if (end < 0) {
            throw new IllegalStateException("The configuration file has no " + XML_ROOT_END);
        }
        StringBuilder indented = new StringBuilder();
        for (String line : snippet.strip().split("\n")) {
            indented.append(" ".repeat(XML_INDENT)).append(line).append('\n');
        }
        return content.substring(0, end) + indented + content.substring(end);
    }

    @SuppressWarnings("unchecked")
    private static String mergeYaml(String content, String sectionName, String snippet) {
        Load load = new Load(LoadSettings.builder().build());
        Map<String, Object> document = (Map<String, Object>) load.loadFromString(content);
        if (document == null) {
            document = new LinkedHashMap<>();
        }
        Map<String, Object> root = (Map<String, Object>) document.computeIfAbsent("hecaton", key -> new LinkedHashMap<>());
        Map<String, Object> section = (Map<String, Object>) root.computeIfAbsent(sectionName, key -> new LinkedHashMap<>());
        section.putAll((Map<String, Object>) load.loadFromString(snippet));
        DumpSettings dumpSettings = DumpSettings.builder().setDefaultFlowStyle(FlowStyle.BLOCK)
                .setIndicatorIndent(0).setIndent(YAML_INDENT).build();
        return new Dump(dumpSettings).dumpToString(document);
    }

    private void backup() throws IOException {
        if (settings.getBackupCount() <= 0) {
            return;
        }
        Path dir = settings.getBackupDir().toPath();
        Files.createDirectories(dir);
        String prefix = file.getFileName() + ".";
        Files.copy(file, dir.resolve(prefix + LocalDateTime.now().format(BACKUP_SUFFIX)), StandardCopyOption.REPLACE_EXISTING);
        List<Path> backups;
        try (Stream<Path> files = Files.list(dir)) {
            backups = files.filter(path -> path.getFileName().toString().startsWith(prefix))
                    .sorted(Comparator.comparing(Path::toString)).toList();
        }
        for (int i = 0; i < backups.size() - settings.getBackupCount(); i++) {
            Files.deleteIfExists(backups.get(i));
        }
    }
}
