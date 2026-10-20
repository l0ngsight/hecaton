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

package uk.org.hecaton.migrate.rewrite;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.openrewrite.maven.Assertions.pomXml;

class HecatonArtifactsTest implements RewriteTest {

    private static final Pattern PARENT = Pattern.compile("(?s)<parent>.*?</parent>");
    private static final Pattern PROJECT_SECTIONS =
            Pattern.compile("(?s)<(dependencies|dependencyManagement|build|profiles|properties)>.*");
    private static final Pattern GROUP_ID = Pattern.compile("<groupId>([^<]+)</groupId>");
    private static final Pattern ARTIFACT_ID = Pattern.compile("<artifactId>([^<]+)</artifactId>");

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new FromHazelcast());
    }

    @Test
    void everyListedArtifactIsBuilt() throws IOException {
        Set<String> built = new TreeSet<>();
        try (Stream<Path> poms = Files.walk(Path.of("..").toAbsolutePath().normalize(), 4)) {
            for (Path pom : poms.filter(p -> p.getFileName().toString().equals("pom.xml"))
                    .filter(p -> !p.toString().contains("/target/") && !p.toString().contains("/src/")).toList()) {
                String xml = Files.readString(pom, StandardCharsets.UTF_8);
                Matcher parent = PARENT.matcher(xml);
                String parentGroup = parent.find() ? first(GROUP_ID, parent.group()) : null;
                String own = PROJECT_SECTIONS.matcher(PARENT.matcher(xml).replaceFirst("")).replaceFirst("");
                String group = first(GROUP_ID, own);
                built.add((group != null ? group : parentGroup) + ":" + first(ARTIFACT_ID, own));
            }
        }
        for (HecatonArtifacts.Artifact a : HecatonArtifacts.ARTIFACTS) {
            assertThat(built).contains(a.hecatonGroupId() + ":" + a.hecatonArtifactId());
        }
    }

    private static String first(Pattern pattern, String text) {
        Matcher m = pattern.matcher(text);
        return m.find() ? m.group(1) : null;
    }

    @Test
    void versionIsHecatons() {
        assertThat(HecatonArtifacts.VERSION).isNotBlank().doesNotContain("${");
    }

    @Test
    void mavenDependencies() {
        rewriteRun(
                pomXml(
                        """
                        <project>
                            <modelVersion>4.0.0</modelVersion>
                            <groupId>com.example</groupId>
                            <artifactId>app</artifactId>
                            <version>1.0</version>
                            <dependencies>
                                <dependency>
                                    <groupId>com.hazelcast</groupId>
                                    <artifactId>hazelcast</artifactId>
                                    <version>5.7.0</version>
                                </dependency>
                            </dependencies>
                        </project>
                        """,
                        """
                        <project>
                            <modelVersion>4.0.0</modelVersion>
                            <groupId>com.example</groupId>
                            <artifactId>app</artifactId>
                            <version>1.0</version>
                            <dependencies>
                                <dependency>
                                    <groupId>uk.org.hecaton</groupId>
                                    <artifactId>hecaton</artifactId>
                                    <version>%s</version>
                                </dependency>
                            </dependencies>
                        </project>
                        """.formatted(HecatonArtifacts.VERSION)
                )
        );
    }
}
