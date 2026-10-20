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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MainTest {

    @TempDir
    Path project;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    @Test
    void dryRunWritesNothing() throws IOException {
        writeSampleProject();

        assertThat(run(project.toString())).isEqualTo(Main.OK);

        assertThat(project.resolve("src/main/resources/hazelcast.yaml")).exists();
        assertThat(Files.readString(project.resolve(".env"))).isEqualTo("HZ_CLUSTERNAME=dev\n");
        assertThat(output())
                .contains("rename src/main/resources/hazelcast.yaml -> src/main/resources/hecaton.yaml")
                .contains("1: - HZ_CLUSTERNAME=dev", "1: + HC_CLUSTERNAME=dev")
                .contains("Would change 2 file(s), would rename 1 file(s).")
                .contains("Skipped 2 Java source and build file(s)")
                .contains("Nothing was written.");
    }

    @Test
    void checkReportsPendingChanges() throws IOException {
        writeSampleProject();

        assertThat(run("--check", project.toString())).isEqualTo(Main.CHANGES_PENDING);
    }

    @Test
    void applyMigratesAndIsIdempotent() throws IOException {
        writeSampleProject();

        assertThat(run("files", "--apply", project.toString())).isEqualTo(Main.OK);

        assertThat(project.resolve("src/main/resources/hazelcast.yaml")).doesNotExist();
        assertThat(Files.readString(project.resolve("src/main/resources/hecaton.yaml")))
                .isEqualTo("hecaton:\n  cluster-name: dev\n");
        assertThat(Files.readString(project.resolve(".env"))).isEqualTo("HC_CLUSTERNAME=dev\n");
        // Java sources and build files are the recipe's job
        assertThat(Files.readString(project.resolve("pom.xml"))).contains("com.hazelcast");
        assertThat(Files.readString(project.resolve("target/hazelcast.yaml"))).isEqualTo("hazelcast:\n");

        assertThat(run("--check", project.toString())).isEqualTo(Main.OK);
    }

    @Test
    void usageErrors() throws IOException {
        assertThat(run()).isEqualTo(Main.USAGE);
        assertThat(run("--bogus", project.toString())).isEqualTo(Main.USAGE);
        assertThat(run("--apply", "--check", project.toString())).isEqualTo(Main.USAGE);
        assertThat(run(project.resolve("missing").toString())).isEqualTo(Main.USAGE);
    }

    private void writeSampleProject() throws IOException {
        write("src/main/resources/hazelcast.yaml", "hazelcast:\n  cluster-name: dev\n");
        write(".env", "HZ_CLUSTERNAME=dev\n");
        write("pom.xml", "<groupId>com.hazelcast</groupId>\n");
        write("src/main/java/App.java", "import com.hazelcast.core.Hazelcast;\n");
        write("README.md", "Uses Hazelcast.\n");
        write("target/hazelcast.yaml", "hazelcast:\n");
    }

    private void write(String path, String content) throws IOException {
        Path file = project.resolve(path);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    private int run(String... args) throws IOException {
        PrintStream ps = new PrintStream(out, true, StandardCharsets.UTF_8);
        return Main.run(args, ps, ps);
    }

    private String output() {
        return out.toString(StandardCharsets.UTF_8);
    }
}
