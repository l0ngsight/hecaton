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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Migrates the non-Java files under one or more directories (spec §5.2): applies
 * {@link TextMigrator} to configuration, properties, environment, container, Kubernetes and
 * script files, and renames the default config files ({@code hazelcast.xml},
 * {@code hazelcast-client.yaml}, ...) that Hecaton looks up by name. Java sources and build
 * files are left to the OpenRewrite recipe.
 * <p>
 * Without {@code apply} nothing is written; the report shows what would change.
 */
public final class FileMigrator {

    private static final Set<String> IGNORED_DIRECTORIES =
            Set.of(".git", ".svn", ".hg", "target", "build", "out", "node_modules", ".gradle", ".idea");
    private static final Pattern DEFAULT_CONFIG_NAME =
            Pattern.compile("hazelcast(-client(?:-failover)?)?\\.(xml|yaml|yml)");

    private final boolean apply;
    private final PrintStream out;
    private int filesChanged;
    private int filesRenamed;
    private int linesToReview;
    private int recipeFiles;

    public FileMigrator(boolean apply, PrintStream out) {
        this.apply = apply;
        this.out = out;
    }

    /** Migrates every file under {@code roots}; returns whether anything was (or would be) changed. */
    public boolean migrate(List<Path> roots) throws IOException {
        for (Path root : roots) {
            for (Path file : listFiles(root)) {
                migrateFile(root, file);
            }
        }
        printSummary();
        return filesChanged + filesRenamed > 0;
    }

    private void migrateFile(Path root, Path file) throws IOException {
        String fileName = file.getFileName().toString();
        FileKind kind = FileKind.of(fileName);
        if (kind == FileKind.RECIPE) {
            recipeFiles++;
            return;
        }
        if (kind == FileKind.SKIPPED) {
            return;
        }
        String display = root.relativize(file).toString().replace('\\', '/');
        byte[] bytes = Files.readAllBytes(file);
        Charset charset = charsetOf(bytes);
        String text = new String(bytes, charset);
        TextMigrator.Result result = TextMigrator.migrate(text, kind);

        Path target = file;
        if (DEFAULT_CONFIG_NAME.matcher(fileName).matches()) {
            Path renamed = file.resolveSibling("hecaton" + fileName.substring("hazelcast".length()));
            if (Files.exists(renamed)) {
                out.println("not renamed, " + root.relativize(renamed) + " exists: " + display);
            } else {
                target = renamed;
                filesRenamed++;
                out.println("rename " + display + " -> " + root.relativize(renamed).toString().replace('\\', '/'));
            }
        }
        if (result.changed(text)) {
            filesChanged++;
            printChangedLines(display, text, result.text());
        }
        for (TextMigrator.Leftover leftover : result.leftovers()) {
            linesToReview++;
            out.println("review " + display + ":" + leftover.line() + ": " + leftover.text());
        }
        if (apply) {
            if (result.changed(text)) {
                Files.write(file, result.text().getBytes(charset));
            }
            if (!target.equals(file)) {
                Files.move(file, target);
            }
        }
    }

    private void printChangedLines(String display, String before, String after) {
        String[] oldLines = before.split("\n", -1);
        String[] newLines = after.split("\n", -1);
        out.println("--- " + display);
        for (int i = 0; i < oldLines.length; i++) {
            if (!oldLines[i].equals(newLines[i])) {
                out.println((i + 1) + ": - " + oldLines[i].strip());
                out.println((i + 1) + ": + " + newLines[i].strip());
            }
        }
    }

    private void printSummary() {
        out.println();
        out.println((apply ? "Changed " : "Would change ") + filesChanged + " file(s), "
                + (apply ? "renamed " : "would rename ") + filesRenamed + " file(s).");
        if (linesToReview > 0) {
            out.println(linesToReview + " line(s) still mention Hazelcast and need review (lines marked \"review\").");
        }
        if (recipeFiles > 0) {
            out.println("Skipped " + recipeFiles + " Java source and build file(s): migrate them with the OpenRewrite "
                    + "recipe uk.org.hecaton.migrate.FromHazelcast.");
        }
        if (!apply && filesChanged + filesRenamed > 0) {
            out.println("Nothing was written. Run again with --apply to make these changes.");
        }
    }

    private static Charset charsetOf(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return StandardCharsets.UTF_8;
        } catch (CharacterCodingException e) {
            return StandardCharsets.ISO_8859_1;
        }
    }

    private static List<Path> listFiles(Path root) throws IOException {
        if (Files.isRegularFile(root)) {
            return List.of(root);
        }
        List<Path> files = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root)) {
            walk.filter(Files::isRegularFile)
                    .filter(p -> !isIgnored(root.relativize(p)))
                    .sorted()
                    .forEach(files::add);
        }
        return files;
    }

    private static boolean isIgnored(Path relative) {
        for (Path part : relative) {
            if (IGNORED_DIRECTORIES.contains(part.toString())) {
                return true;
            }
        }
        return false;
    }
}
