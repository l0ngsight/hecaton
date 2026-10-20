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

import uk.org.hecaton.migrate.HecatonNames;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Applies the Hazelcast → Hecaton renames (spec §5.2) to the text of one non-Java file.
 * <p>
 * Only names that Hecaton reads are renamed: the config namespace and schema files, XML
 * root elements and YAML root keys, {@code com.hazelcast} class names, known property keys
 * and file names ({@link HecatonNames#renameKnownName}), {@code HZ_}/{@code HZCLIENT_}
 * environment variables, {@code -Dhz.} overrides, {@code hz-*} scripts, the Spring Boot
 * {@code spring.hazelcast.config} key and the SQL catalog. URLs, e-mail addresses, domain
 * names and copyright lines are never changed. Every other "hazelcast" is left as is and
 * reported for review: it is either an application's own name or something without a
 * Hecaton equivalent (a Hazelcast container image, for example).
 * <p>
 * The rules never add or remove line breaks, so the result has the same lines as the input.
 */
public final class TextMigrator {

    private static final Pattern HAZELCAST_ANY_CASE = Pattern.compile("(?i)hazelcast");

    private static final List<Pattern> PROTECT = List.of(
            Pattern.compile("(?i)Copyright[^\\n]*"),
            Pattern.compile("Hazelcast,? Inc\\.?"),
            // URLs, except the config schema namespace and schema files
            Pattern.compile("(?i)\\b(?:https?|ftp)://(?!www\\.hazelcast\\.com/schema/)[^\\s\"'<>(){}\\[\\]\\\\`|]+"),
            Pattern.compile("[\\w.+-]+@[\\w-]+(?:\\.[\\w-]+)+"),
            // bare domains (but not property keys such as hazelcast.io.x)
            Pattern.compile("(?i)\\b[\\w.-]*hazelcast\\.(?:com|org|io|cloud|net|dev)(?!\\w|\\.\\w|/schema/)"),
            Pattern.compile("(?i)\\bgithub\\.com[:/]hazelcast[\\w./-]*"),
            // third-party packages that carry the Hazelcast name
            Pattern.compile("com\\.hazelcast\\.(?:jsurfer|maven)\\b"));

    private static final Pattern SCHEMA_NAMESPACE = Pattern.compile("https?://www\\.hazelcast\\.com/schema/");
    private static final Pattern SCHEMA_FILE = Pattern.compile("\\bhazelcast-([\\w.-]*\\.xsd)\\b");
    private static final Pattern PACKAGE = Pattern.compile("\\bcom([./])hazelcast\\b");
    private static final Pattern HECATON_CLASS = Pattern.compile("\\buk([./])org\\1hecaton(?:\\1[\\w$]+)+");
    private static final Pattern XML_ROOT =
            Pattern.compile("<(/?)((?:[\\w-]+:)?)hazelcast(-client(?:-failover)?)?(?=[\\s>/])");
    private static final Pattern YAML_ROOT = Pattern.compile("(?m)^hazelcast(-client(?:-failover)?)?:(?=\\s|$)");
    private static final Pattern KNOWN_NAME =
            Pattern.compile("(?:(?<=-D)|(?<![\\w$.-]))hazelcast[.-][A-Za-z0-9_.-]*[A-Za-z0-9_]");
    private static final Pattern SPRING_BOOT_KEY = Pattern.compile("\\b(spring[._-])hazelcast([._-]config)\\b");
    private static final Pattern SPRING_BOOT_ENV = Pattern.compile("\\bSPRING_HAZELCAST_CONFIG\\b");
    private static final Pattern SQL_CATALOG = Pattern.compile("\\bhazelcast\\.public\\.");
    private static final Pattern ENV_PREFIX = Pattern.compile("(?<![\\w$])HZ(CLIENT)?_(?=[A-Z])(?!INSTANCE_TRACKING)");
    private static final Pattern SYSTEM_PROPERTY_OVERRIDE = Pattern.compile("-Dhz\\.");
    private static final Pattern SCRIPT = Pattern.compile("\\bhz-(start|stop|cli|cluster-admin|cluster-cp-admin|healthcheck)\\b");
    private static final Pattern BIN_HZ = Pattern.compile("(?<=/bin/)hz(?=\\s|$|[\"';&|])");

    /**
     * A line with a "hazelcast" that no rule renamed, for the user to review.
     *
     * @param line the line number, from 1
     * @param text the line, without surrounding white space
     */
    public record Leftover(int line, String text) {
    }

    /**
     * The migrated text and the leftovers in it.
     *
     * @param text      the migrated text
     * @param leftovers lines that still mention Hazelcast outside protected spans
     */
    public record Result(String text, List<Leftover> leftovers) {
        public boolean changed(String original) {
            return !text.equals(original);
        }
    }

    private TextMigrator() {
    }

    public static Result migrate(String text, FileKind kind) {
        String renamed = transformUnprotected(text, segment -> renameSegment(segment, kind));
        if (kind == FileKind.YAML) {
            renamed = SpringBootYaml.renameNestedKey(renamed);
        }
        return new Result(renamed, leftovers(renamed));
    }

    private static String renameSegment(String s, FileKind kind) {
        s = SCHEMA_NAMESPACE.matcher(s).replaceAll("https://hecaton.org.uk/schema/");
        s = SCHEMA_FILE.matcher(s).replaceAll("hecaton-$1");
        s = PACKAGE.matcher(s).replaceAll("uk$1org$1hecaton");
        s = replace(HECATON_CLASS, s, m -> HecatonNames.renameIdentifier(m.group()));
        if (kind == FileKind.XML) {
            s = XML_ROOT.matcher(s).replaceAll("<$1$2hecaton$3");
        }
        if (kind == FileKind.YAML) {
            s = YAML_ROOT.matcher(s).replaceAll("hecaton$1:");
        }
        s = SPRING_BOOT_KEY.matcher(s).replaceAll("$1hecaton$2");
        s = SPRING_BOOT_ENV.matcher(s).replaceAll("SPRING_HECATON_CONFIG");
        s = SQL_CATALOG.matcher(s).replaceAll("hecaton.public.");
        s = replace(KNOWN_NAME, s, m -> {
            String hecaton = HecatonNames.renameKnownName(m.group());
            return hecaton != null ? hecaton : m.group();
        });
        s = ENV_PREFIX.matcher(s).replaceAll("HC$1_");
        s = SYSTEM_PROPERTY_OVERRIDE.matcher(s).replaceAll("-Dhc.");
        s = SCRIPT.matcher(s).replaceAll("hc-$1");
        s = BIN_HZ.matcher(s).replaceAll("hc");
        return s;
    }

    private static String replace(Pattern pattern, String s, Function<MatchResult, String> replacement) {
        return pattern.matcher(s).replaceAll(m -> Matcher.quoteReplacement(replacement.apply(m)));
    }

    /** Applies {@code fn} to the text outside protected spans. */
    static String transformUnprotected(String text, Function<String, String> fn) {
        List<int[]> spans = protectedSpans(text);
        StringBuilder out = new StringBuilder(text.length());
        int pos = 0;
        for (int[] span : spans) {
            out.append(fn.apply(text.substring(pos, span[0])));
            out.append(text, span[0], span[1]);
            pos = span[1];
        }
        out.append(fn.apply(text.substring(pos)));
        return out.toString();
    }

    /** Sorted, merged protected spans. */
    private static List<int[]> protectedSpans(String text) {
        List<int[]> spans = new ArrayList<>();
        for (Pattern p : PROTECT) {
            Matcher m = p.matcher(text);
            while (m.find()) {
                if (m.end() > m.start()) {
                    spans.add(new int[] {m.start(), m.end()});
                }
            }
        }
        spans.sort((a, b) -> a[0] != b[0] ? Integer.compare(a[0], b[0]) : Integer.compare(b[1], a[1]));
        List<int[]> merged = new ArrayList<>();
        for (int[] s : spans) {
            if (!merged.isEmpty() && s[0] <= merged.get(merged.size() - 1)[1]) {
                int[] last = merged.get(merged.size() - 1);
                last[1] = Math.max(last[1], s[1]);
            } else {
                merged.add(s.clone());
            }
        }
        return merged;
    }

    private static List<Leftover> leftovers(String text) {
        List<Leftover> result = new ArrayList<>();
        if (!HAZELCAST_ANY_CASE.matcher(text).find()) {
            return result;
        }
        List<int[]> spans = protectedSpans(text);
        String[] lines = text.split("\n", -1);
        int lineStart = 0;
        for (int i = 0; i < lines.length; i++) {
            Matcher m = HAZELCAST_ANY_CASE.matcher(lines[i]);
            while (m.find()) {
                int at = lineStart + m.start();
                int end = lineStart + m.end();
                if (spans.stream().noneMatch(s -> s[0] <= at && end <= s[1])) {
                    result.add(new Leftover(i + 1, lines[i].strip()));
                    break;
                }
            }
            lineStart += lines[i].length() + 1;
        }
        return result;
    }
}
