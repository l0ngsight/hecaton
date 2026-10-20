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

import uk.org.hecaton.migrate.data.DataCommand;
import uk.org.hecaton.migrate.files.FileMigrator;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Command line entry point.
 *
 * <pre>
 * hecaton-migrate [files] [--apply | --check] PATH...
 * hecaton-migrate data --source SOURCE --target TARGET [OPTIONS]
 * </pre>
 */
public final class Main {

    static final int OK = 0;
    static final int CHANGES_PENDING = 1;
    static final int USAGE = 2;

    private static final String USAGE_TEXT = """
            Usage: hecaton-migrate [files] [--apply | --check] PATH...
                   hecaton-migrate data --source SOURCE --target TARGET [OPTIONS]  (see data --help)

            Migrates the configuration, properties, environment, Dockerfile, Kubernetes and
            script files under each PATH from Hazelcast to Hecaton. Java sources and build files
            are migrated by the OpenRewrite recipe uk.org.hecaton.migrate.FromHazelcast.

              (no option)  show what would change; nothing is written
              --apply      make the changes
              --check      like the default, but exit with status 1 if anything would change
            """;

    private Main() {
    }

    public static void main(String[] args) throws IOException {
        System.exit(run(args, System.out, System.err));
    }

    static int run(String[] args, PrintStream out, PrintStream err) throws IOException {
        if (args.length > 0 && args[0].equals("data")) {
            List<String> rest = List.of(args).subList(1, args.length);
            if (rest.contains("-h") || rest.contains("--help")) {
                out.print(DataCommand.USAGE);
                return OK;
            }
            return DataCommand.run(rest, out, err);
        }
        if (List.of(args).contains("-h") || List.of(args).contains("--help")) {
            out.print(USAGE_TEXT);
            return OK;
        }
        Options options = Options.parse(args);
        if (options.error() != null) {
            err.println(options.error());
            err.print(USAGE_TEXT);
            return USAGE;
        }
        boolean changes = new FileMigrator(options.apply(), out).migrate(options.paths());
        return options.check() && changes ? CHANGES_PENDING : OK;
    }

    /** Parsed command line; {@code error} is set if it is not valid. */
    private record Options(boolean apply, boolean check, List<Path> paths, String error) {

        static Options parse(String[] args) {
            boolean apply = false;
            boolean check = false;
            List<Path> paths = new ArrayList<>();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.equals("--apply")) {
                    apply = true;
                } else if (arg.equals("--check")) {
                    check = true;
                } else if (arg.startsWith("-")) {
                    return invalid("Unknown option: " + arg);
                } else if (i > 0 || !arg.equals("files")) {
                    paths.add(Path.of(arg));
                }
            }
            if (paths.isEmpty() || apply && check) {
                return invalid("Give one or more paths, and at most one of --apply and --check.");
            }
            for (Path path : paths) {
                if (!Files.exists(path)) {
                    return invalid("No such file or directory: " + path);
                }
            }
            return new Options(apply, check, paths, null);
        }

        private static Options invalid(String error) {
            return new Options(false, false, List.of(), error);
        }
    }
}
