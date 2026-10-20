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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Turns an upstream Hazelcast source tree into the Hecaton tree, driven by
 * {@code rename-table.yaml} (spec §5, implementation plan §1.1).
 *
 * <pre>
 * java tools/rebrand/Rebrand.java [--table FILE] [--report FILE] SOURCE_DIR TARGET_DIR
 * java tools/rebrand/Rebrand.java [--table FILE] --check DIR
 * </pre>
 *
 * The first form writes the renamed copy of SOURCE_DIR into TARGET_DIR, which must not
 * contain anything but an optional {@code .git} entry. The second form verifies that DIR is
 * already fully renamed: applying the table again changes nothing (idempotence) and every
 * remaining "hazelcast" sits inside a protected span.
 *
 * Both forms exit with status 1 if a "hazelcast" occurrence is left outside protected spans.
 *
 * Jar files (test fixtures) are rewritten too: entry paths are renamed, text entries get
 * the content rules, and class files get the content rules applied to every CONSTANT_Utf8
 * entry of their constant pool (class names, descriptors, string literals). Other binary
 * files are copied unchanged.
 */
public final class Rebrand {

    private static final Pattern HAZELCAST_ANY_CASE = Pattern.compile("(?i)hazelcast");
    private static final Set<String> BINARY_EXTENSIONS = Set.of(
            "jar", "class", "png", "jpg", "jpeg", "gif", "ico", "jks", "p12", "pfx", "keystore",
            "truststore", "bin", "gz", "zip", "tgz", "so", "dll", "dylib", "ser", "der", "jfr", "parquet",
            "avro", "orc", "xlsx", "woff", "woff2", "ttf", "eot", "pdf");
    /** Always treated as text, even if they contain NUL bytes (e.g. in string literals). */
    private static final Set<String> TEXT_EXTENSIONS = Set.of(
            "java", "kt", "xml", "xsd", "yaml", "yml", "json", "properties", "md", "txt", "sh", "bat", "cmd",
            "c", "h", "py", "proto", "sql", "ftl", "fmpp", "jj", "html", "css", "js", "toml", "adoc");

    record Rule(String name, Pattern from, String to, boolean content, boolean path, List<PathMatcher> paths) {
        boolean appliesTo(String upstreamPath) {
            return paths.isEmpty() || paths.stream().anyMatch(m -> m.matches(Path.of(upstreamPath)));
        }
    }

    record Table(List<Pattern> protect, List<Rule> rules, List<Rule> relocate, List<PathMatcher> keepPaths,
                 List<PathMatcher> codecHashPaths) {
    }

    /** The integrity hash generated client-protocol codecs carry (see CodecHashTest). */
    private static final Pattern CODEC_HASH = Pattern.compile("@Generated\\(\"([a-f0-9]{32})\"\\)");

    record Leftover(String path, int line, String text) {
    }

    private final Table table;
    private final List<Leftover> leftovers = new ArrayList<>();
    private final Map<String, Integer> protectedHits = new TreeMap<>();
    private int filesChanged;
    private int filesRenamed;
    private int jarsRewritten;

    private Rebrand(Table table) {
        this.table = table;
    }

    public static void main(String[] args) throws IOException {
        Path tablePath = Path.of("tools/rebrand/rename-table.yaml");
        Path reportPath = null;
        Path check = null;
        List<String> positional = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--table" -> tablePath = Path.of(args[++i]);
                case "--report" -> reportPath = Path.of(args[++i]);
                case "--check" -> check = Path.of(args[++i]);
                default -> positional.add(args[i]);
            }
        }
        Rebrand rebrand = new Rebrand(TableParser.parse(tablePath));
        boolean ok;
        if (check != null) {
            ok = rebrand.check(check);
        } else if (positional.size() == 2) {
            ok = rebrand.run(Path.of(positional.get(0)), Path.of(positional.get(1)));
        } else {
            System.err.println("usage: Rebrand [--table FILE] [--report FILE] SOURCE_DIR TARGET_DIR");
            System.err.println("       Rebrand [--table FILE] --check DIR");
            System.exit(2);
            return;
        }
        String report = rebrand.report();
        System.out.print(report);
        if (reportPath != null) {
            Files.writeString(reportPath, report, StandardCharsets.UTF_8);
        }
        System.exit(ok ? 0 : 1);
    }

    // ------------------------------------------------------------------------------------
    // Modes

    private boolean run(Path source, Path target) throws IOException {
        Files.createDirectories(target);
        try (Stream<Path> entries = Files.list(target)) {
            if (entries.anyMatch(p -> !p.getFileName().toString().equals(".git"))) {
                throw new IllegalArgumentException("target directory is not empty: " + target);
            }
        }
        Map<String, String> outputPaths = new TreeMap<>();
        for (String rel : listFiles(source)) {
            Path in = source.resolve(rel);
            String relocated = relocate(rel);
            boolean keep = relocated != null || matchesAny(table.keepPaths(), rel);
            String outRel = relocated != null ? relocated : keep ? rel : renamePath(rel);
            String previous = outputPaths.put(outRel, rel);
            if (previous != null) {
                throw new IllegalStateException("rename collision: " + previous + " and " + rel + " -> " + outRel);
            }
            if (!outRel.equals(rel)) {
                filesRenamed++;
            }
            Path out = target.resolve(outRel);
            Files.createDirectories(out.getParent());
            if (Files.isSymbolicLink(in)) {
                Files.createSymbolicLink(out, Files.readSymbolicLink(in));
                continue;
            }
            byte[] bytes = Files.readAllBytes(in);
            if (!keep && rel.endsWith(".jar")) {
                byte[] renamed = renameJar(bytes);
                if (!Arrays.equals(renamed, bytes)) {
                    filesChanged++;
                    jarsRewritten++;
                    bytes = renamed;
                }
            } else if (!keep && !isBinary(rel, bytes)) {
                String text = new String(bytes, StandardCharsets.ISO_8859_1);
                String renamed = transformContent(rel, text);
                if (!renamed.equals(text)) {
                    filesChanged++;
                    bytes = renamed.getBytes(StandardCharsets.ISO_8859_1);
                }
                collectLeftovers(outRel, renamed);
            }
            Files.write(out, bytes);
            copyPermissions(in, out);
        }
        return leftovers.isEmpty();
    }

    private boolean check(Path dir) throws IOException {
        List<String> notIdempotent = new ArrayList<>();
        for (String rel : listFiles(dir)) {
            if (matchesAny(table.keepPaths(), rel)) {
                continue;
            }
            if (!renamePath(rel).equals(rel)) {
                notIdempotent.add(rel + " (path)");
            }
            Path file = dir.resolve(rel);
            if (Files.isSymbolicLink(file)) {
                continue;
            }
            byte[] bytes = Files.readAllBytes(file);
            if (rel.endsWith(".jar")) {
                if (!Arrays.equals(renameJar(bytes), bytes)) {
                    notIdempotent.add(rel);
                }
                continue;
            }
            if (isBinary(rel, bytes)) {
                continue;
            }
            String text = new String(bytes, StandardCharsets.ISO_8859_1);
            if (!transformContent(rel, text).equals(text)) {
                notIdempotent.add(rel);
            }
            collectLeftovers(rel, text);
        }
        notIdempotent.forEach(p -> System.out.println("not idempotent: " + p));
        return notIdempotent.isEmpty() && leftovers.isEmpty();
    }

    // ------------------------------------------------------------------------------------
    // Renaming

    /** The relocated path, or {@code null} if no relocate rule matches. */
    String relocate(String rel) {
        for (Rule rule : table.relocate()) {
            Matcher m = rule.from().matcher(rel);
            if (m.find()) {
                return m.replaceFirst(rule.to());
            }
        }
        return null;
    }

    String renamePath(String rel) {
        String result = rel;
        for (Rule rule : table.rules()) {
            if (rule.path() && rule.appliesTo(rel)) {
                result = rule.from().matcher(result).replaceAll(rule.to());
            }
        }
        return result;
    }

    /** Content rules plus derived values that depend on the renamed content (codec hashes). */
    String transformContent(String rel, String text) {
        String renamed = renameContent(rel, text);
        return matchesAny(table.codecHashPaths(), rel) ? regenerateCodecHash(renamed) : renamed;
    }

    /**
     * Recomputes the {@code @Generated("<md5>")} hash of a client-protocol codec the way
     * CodecHashTest does: MD5 over the lines joined with "\n" (trailing newline included),
     * with the hash itself replaced by {@code !codec_hash!}.
     */
    static String regenerateCodecHash(String content) {
        Matcher m = CODEC_HASH.matcher(content);
        if (!m.find()) {
            return content;
        }
        StringBuilder normalized = new StringBuilder(content.length() + 1);
        content.lines().forEach(line -> normalized.append(line).append('\n'));
        String hashInput = CODEC_HASH.matcher(normalized).replaceFirst("@Generated(\"!codec_hash!\")");
        try {
            byte[] digest = MessageDigest.getInstance("MD5")
                    .digest(hashInput.getBytes(StandardCharsets.ISO_8859_1));
            return content.substring(0, m.start(1)) + HexFormat.of().formatHex(digest) + content.substring(m.end(1));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    String renameContent(String rel, String text) {
        List<int[]> spans = protectedSpans(text, null);
        StringBuilder out = new StringBuilder(text.length());
        int pos = 0;
        for (int[] span : spans) {
            out.append(renameSegment(rel, text.substring(pos, span[0])));
            out.append(text, span[0], span[1]);
            pos = span[1];
        }
        out.append(renameSegment(rel, text.substring(pos)));
        return out.toString();
    }

    private String renameSegment(String rel, String segment) {
        if (segment.isEmpty()) {
            return segment;
        }
        String result = segment;
        for (Rule rule : table.rules()) {
            if (rule.content() && rule.appliesTo(rel)) {
                result = rule.from().matcher(result).replaceAll(rule.to());
            }
        }
        return result;
    }

    /**
     * Rewrites a jar: entry paths, text entries and class-file constant pools. Returns the
     * input unchanged when no entry needs renaming.
     */
    byte[] renameJar(byte[] jar) throws IOException {
        boolean changed = false;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(jar.length + 1024);
        try (ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(jar));
             ZipOutputStream out = new ZipOutputStream(bytes)) {
            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                byte[] original = in.readAllBytes();
                byte[] data = original;
                String name = entry.getName();
                if (!entry.isDirectory()) {
                    if (name.endsWith(".class")) {
                        data = renameClassFile(name, data);
                    } else if (!isBinary(name, data)) {
                        String text = new String(data, StandardCharsets.ISO_8859_1);
                        data = renameContent(name, text).getBytes(StandardCharsets.ISO_8859_1);
                    }
                }
                String newName = renamePath(name);
                changed |= !newName.equals(name) || !Arrays.equals(data, original);
                ZipEntry renamed = new ZipEntry(newName);
                // Local date-time keeps the output independent of the time zone.
                renamed.setTimeLocal(entry.getTimeLocal());
                out.putNextEntry(renamed);
                out.write(data);
                out.closeEntry();
            }
        }
        return changed ? bytes.toByteArray() : jar;
    }

    /**
     * Applies the content rules to every CONSTANT_Utf8 entry of a class file. Constant-pool
     * entries are length-prefixed and referenced by index only, so the rest of the class file
     * is copied unchanged.
     */
    byte[] renameClassFile(String name, byte[] c) {
        if (c.length < 10 || (c[0] & 0xff) != 0xCA || (c[1] & 0xff) != 0xFE
                || (c[2] & 0xff) != 0xBA || (c[3] & 0xff) != 0xBE) {
            return c;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(c.length + 256);
        out.write(c, 0, 10);
        int count = u2(c, 8);
        int pos = 10;
        for (int i = 1; i < count; i++) {
            int tag = c[pos] & 0xff;
            int size = switch (tag) {
                case 1 -> -1;
                case 7, 8, 16, 19, 20 -> 3;
                case 15 -> 4;
                case 3, 4, 9, 10, 11, 12, 17, 18 -> 5;
                case 5, 6 -> 9;
                default -> throw new IllegalStateException(name + ": unknown constant-pool tag " + tag);
            };
            if (size < 0) {
                int length = u2(c, pos + 1);
                String value = new String(c, pos + 3, length, StandardCharsets.ISO_8859_1);
                byte[] renamed = renameContent(name, value).getBytes(StandardCharsets.ISO_8859_1);
                if (renamed.length > 0xFFFF) {
                    throw new IllegalStateException(name + ": constant too long after renaming");
                }
                out.write(1);
                out.write(renamed.length >>> 8);
                out.write(renamed.length & 0xff);
                out.write(renamed, 0, renamed.length);
                pos += 3 + length;
            } else {
                out.write(c, pos, size);
                pos += size;
                if (tag == 5 || tag == 6) {
                    i++; // long and double take two constant-pool slots
                }
            }
        }
        out.write(c, pos, c.length - pos);
        return out.toByteArray();
    }

    private static int u2(byte[] b, int at) {
        return ((b[at] & 0xff) << 8) | (b[at + 1] & 0xff);
    }

    /** Sorted, merged protected spans. When {@code hits} is given, counts matches per rule. */
    private List<int[]> protectedSpans(String text, Map<String, Integer> hits) {
        List<int[]> spans = new ArrayList<>();
        for (Pattern p : table.protect()) {
            Matcher m = p.matcher(text);
            while (m.find()) {
                if (m.end() > m.start()) {
                    spans.add(new int[] {m.start(), m.end()});
                    if (hits != null && HAZELCAST_ANY_CASE.matcher(m.group()).find()) {
                        hits.merge(p.pattern(), 1, Integer::sum);
                    }
                }
            }
        }
        spans.sort((a, b) -> a[0] != b[0] ? Integer.compare(a[0], b[0]) : Integer.compare(b[1], a[1]));
        List<int[]> merged = new ArrayList<>();
        for (int[] s : spans) {
            if (!merged.isEmpty() && s[0] <= merged.getLast()[1]) {
                merged.getLast()[1] = Math.max(merged.getLast()[1], s[1]);
            } else {
                merged.add(s.clone());
            }
        }
        return merged;
    }

    private void collectLeftovers(String rel, String text) {
        if (!HAZELCAST_ANY_CASE.matcher(text).find()) {
            return;
        }
        List<int[]> spans = protectedSpans(text, protectedHits);
        Matcher m = HAZELCAST_ANY_CASE.matcher(text);
        while (m.find()) {
            int at = m.start();
            boolean inside = spans.stream().anyMatch(s -> s[0] <= at && m.end() <= s[1]);
            if (!inside) {
                int lineStart = text.lastIndexOf('\n', at) + 1;
                int lineEnd = text.indexOf('\n', at);
                String line = text.substring(lineStart, lineEnd < 0 ? text.length() : lineEnd).strip();
                int lineNo = (int) text.substring(0, at).chars().filter(c -> c == '\n').count() + 1;
                leftovers.add(new Leftover(rel, lineNo, line.length() > 160 ? line.substring(0, 160) + "..." : line));
            }
        }
        String renamedPath = rel;
        if (HAZELCAST_ANY_CASE.matcher(renamedPath).find() && !matchesAny(table.keepPaths(), rel)) {
            leftovers.add(new Leftover(rel, 0, "(path)"));
        }
    }

    // ------------------------------------------------------------------------------------
    // Helpers

    private static List<String> listFiles(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk
                    .filter(p -> !p.equals(root))
                    .filter(p -> !root.relativize(p).startsWith(".git"))
                    .filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(p))
                    .map(p -> root.relativize(p).toString().replace('\\', '/'))
                    .sorted()
                    .toList();
        }
    }

    private static boolean isBinary(String rel, byte[] bytes) {
        int dot = rel.lastIndexOf('.');
        String extension = dot >= 0 ? rel.substring(dot + 1).toLowerCase() : "";
        if (TEXT_EXTENSIONS.contains(extension)) {
            return false;
        }
        if (BINARY_EXTENSIONS.contains(extension)) {
            return true;
        }
        int limit = Math.min(bytes.length, 8192);
        for (int i = 0; i < limit; i++) {
            if (bytes[i] == 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesAny(List<PathMatcher> matchers, String rel) {
        Path p = Path.of(rel);
        return matchers.stream().anyMatch(m -> m.matches(p));
    }

    private static void copyPermissions(Path from, Path to) {
        try {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(from);
            Files.setPosixFilePermissions(to, perms);
        } catch (UnsupportedOperationException e) {
            // non-POSIX file system: keep defaults
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String report() {
        StringBuilder sb = new StringBuilder();
        sb.append("# Rebrand report\n\n");
        sb.append("- files with renamed content: ").append(filesChanged).append('\n');
        sb.append("- files with renamed paths: ").append(filesRenamed).append('\n');
        sb.append("- jars rewritten: ").append(jarsRewritten).append('\n');
        sb.append("- leftovers outside protected spans: ").append(leftovers.size()).append("\n\n");
        sb.append("## Remaining \"hazelcast\" occurrences inside protected spans, by rule\n\n");
        sb.append("| Matches | Protect rule |\n|---|---|\n");
        protectedHits.forEach((rule, n) -> sb.append("| ").append(n).append(" | `")
                .append(rule.replace("|", "\\|")).append("` |\n"));
        if (!leftovers.isEmpty()) {
            sb.append("\n## Leftovers (must be fixed in rename-table.yaml)\n\n");
            Map<String, List<Leftover>> byFile = new LinkedHashMap<>();
            leftovers.forEach(l -> byFile.computeIfAbsent(l.path(), k -> new ArrayList<>()).add(l));
            byFile.forEach((file, list) -> list.stream().limit(5).forEach(l ->
                    sb.append("- `").append(file).append(':').append(l.line()).append("` ").append(l.text()).append('\n')));
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------------------------
    // Minimal YAML subset parser for rename-table.yaml: top-level keys holding lists of
    // scalars or of flat maps; scalars may be plain, 'single' or "double" quoted; flow
    // lists [a, b] of quoted or plain scalars.

    static final class TableParser {

        static Table parse(Path file) throws IOException {
            Map<String, List<Object>> top = new LinkedHashMap<>();
            List<Object> current = null;
            Map<String, String> currentMap = null;
            int lineNo = 0;
            for (String raw : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                lineNo++;
                String line = stripComment(raw);
                if (line.isBlank()) {
                    continue;
                }
                int indent = raw.length() - raw.stripLeading().length();
                String body = line.strip();
                if (indent == 0) {
                    int colon = body.indexOf(':');
                    String key = body.substring(0, colon).strip();
                    String value = body.substring(colon + 1).strip();
                    current = new ArrayList<>();
                    currentMap = null;
                    top.put(key, current);
                    if (!value.isEmpty()) {
                        current.add(scalar(value));
                    }
                } else if (body.startsWith("- ")) {
                    String item = body.substring(2).strip();
                    if (isMapEntry(item)) {
                        currentMap = new LinkedHashMap<>();
                        current.add(currentMap);
                        putEntry(currentMap, item);
                    } else {
                        currentMap = null;
                        current.add(scalar(item));
                    }
                } else if (currentMap != null && isMapEntry(body)) {
                    putEntry(currentMap, body);
                } else {
                    throw new IllegalArgumentException(file + ":" + lineNo + ": unsupported YAML: " + raw);
                }
            }

            List<Pattern> protect = new ArrayList<>();
            for (Object o : top.getOrDefault("protect", List.of())) {
                protect.add(Pattern.compile((String) o));
            }
            List<Rule> rules = rules(top.getOrDefault("replace", List.of()));
            List<Rule> relocate = rules(top.getOrDefault("relocate", List.of()));
            return new Table(protect, rules, relocate, globs(top.get("keep-paths")),
                    globs(top.get("regenerate-codec-hash")));
        }

        private static List<PathMatcher> globs(List<Object> entries) {
            List<PathMatcher> matchers = new ArrayList<>();
            for (Object o : entries == null ? List.of() : entries) {
                matchers.add(FileSystems.getDefault().getPathMatcher("glob:" + o));
            }
            return matchers;
        }

        private static List<Rule> rules(List<Object> entries) {
            List<Rule> rules = new ArrayList<>();
            for (Object o : entries) {
                @SuppressWarnings("unchecked")
                Map<String, String> m = (Map<String, String>) o;
                String scope = m.getOrDefault("scope", "content,path");
                List<PathMatcher> paths = new ArrayList<>();
                for (String glob : flowList(m.get("paths"))) {
                    paths.add(FileSystems.getDefault().getPathMatcher("glob:" + glob));
                }
                rules.add(new Rule(m.get("from"), Pattern.compile(m.get("from")), m.get("to"),
                        scope.contains("content"), scope.contains("path"), paths));
            }
            return rules;
        }

        private static boolean isMapEntry(String s) {
            return s.matches("[A-Za-z_-]+:(\\s.*|$)");
        }

        private static void putEntry(Map<String, String> map, String entry) {
            int colon = entry.indexOf(':');
            map.put(entry.substring(0, colon).strip(), scalarOrRaw(entry.substring(colon + 1).strip()));
        }

        private static String scalarOrRaw(String value) {
            return value.startsWith("[") ? value : scalar(value);
        }

        private static List<String> flowList(String value) {
            if (value == null || value.isBlank()) {
                return List.of();
            }
            String inner = value.strip();
            if (!inner.startsWith("[") || !inner.endsWith("]")) {
                return List.of(scalar(inner));
            }
            List<String> items = new ArrayList<>();
            for (String part : inner.substring(1, inner.length() - 1).split(",")) {
                if (!part.isBlank()) {
                    items.add(scalar(part.strip()));
                }
            }
            return items;
        }

        private static String scalar(String value) {
            if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
                return value.substring(1, value.length() - 1).replace("''", "'");
            }
            if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
                return value.substring(1, value.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
            }
            return value;
        }

        /** Removes a trailing # comment that is outside quotes. */
        private static String stripComment(String line) {
            boolean single = false;
            boolean dbl = false;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '\'' && !dbl) {
                    single = !single;
                } else if (c == '"' && !single) {
                    dbl = !dbl;
                } else if (c == '#' && !single && !dbl && (i == 0 || Character.isWhitespace(line.charAt(i - 1)))) {
                    return line.substring(0, i);
                }
            }
            return line;
        }
    }
}
