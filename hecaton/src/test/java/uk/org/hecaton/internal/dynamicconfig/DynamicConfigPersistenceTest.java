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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.config.ConfigNamespace;
import uk.org.hecaton.internal.config.ConfigSections;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Dynamic configuration persistence and configuration reload/update (F16).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class DynamicConfigPersistenceTest extends HecatonTestSupport {

    private static final String XML = """
            <hecaton xmlns="https://hecaton.org.uk/schema/config">
                <cluster-name>persisted</cluster-name>
                <dynamic-configuration>
                    <persistence-enabled>true</persistence-enabled>
                    <backup-dir>%s</backup-dir>
                    <backup-count>2</backup-count>
                </dynamic-configuration>
                <map name="static">
                    <backup-count>1</backup-count>
                </map>
            </hecaton>
            """;

    private static final String YAML = """
            hecaton:
              cluster-name: persisted
              dynamic-configuration:
                persistence-enabled: true
                backup-dir: %s
                backup-count: 2
              map:
                static:
                  backup-count: 1
            """;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void dynamicConfigurationsAreWrittenToTheXmlFile() throws Exception {
        configurationsSurviveARestart("hecaton.xml", XML);
    }

    @Test
    public void dynamicConfigurationsAreWrittenToTheYamlFile() throws Exception {
        configurationsSurviveARestart("hecaton.yaml", YAML);
    }

    private void configurationsSurviveARestart(String fileName, String template) throws Exception {
        File backups = folder.newFolder("backups");
        File file = folder.newFile(fileName);
        Files.writeString(file.toPath(), template.formatted(backups.getAbsolutePath()));

        HecatonInstance member = factory.newHecatonInstance(Config.loadFromFile(file));
        member.getConfig().addMapConfig(new MapConfig("dynamic").setBackupCount(3).setTimeToLiveSeconds(60));
        member.getConfig().addQueueConfig(new QueueConfig("jobs").setMaxSize(100));
        member.getConfig().addQueueConfig(new QueueConfig("more-jobs"));

        // three changes, two backups kept
        assertThat(backups.list()).hasSize(2);
        member.shutdown();

        Config restarted = Config.loadFromFile(file);
        assertThat(restarted.getMapConfigs().get("dynamic").getBackupCount()).isEqualTo(3);
        assertThat(restarted.getMapConfigs().get("dynamic").getTimeToLiveSeconds()).isEqualTo(60);
        assertThat(restarted.getQueueConfigs().get("jobs").getMaxSize()).isEqualTo(100);
        assertThat(restarted.getQueueConfigs()).containsKey("more-jobs");
        assertThat(restarted.getMapConfigs().get("static").getBackupCount()).isEqualTo(1);

        // a member started from the file does not write the configurations again
        String content = Files.readString(file.toPath());
        HecatonInstance again = factory.newHecatonInstance(restarted);
        again.getConfig().addMapConfig(new MapConfig("dynamic").setBackupCount(3).setTimeToLiveSeconds(60));
        assertThat(Files.readString(file.toPath())).isEqualTo(content);
    }

    @Test
    public void reloadAddsNewConfigurationsAndIgnoresChangedOnes() throws Exception {
        File file = folder.newFile("hecaton.yaml");
        Files.writeString(file.toPath(), """
                hecaton:
                  cluster-name: reloaded
                  map:
                    orders:
                      backup-count: 1
                """);
        HecatonInstance member = factory.newHecatonInstance(Config.loadFromFile(file));
        Files.writeString(file.toPath(), """
                hecaton:
                  cluster-name: reloaded
                  map:
                    orders:
                      backup-count: 2
                    payments:
                      backup-count: 2
                  queue:
                    jobs:
                      max-size: 10
                """);

        ConfigUpdateResult result = getNodeEngineImpl(member).getConfigurationService().update();

        assertThat(result.getAddedConfigs()).containsExactlyInAnyOrder(
                new ConfigNamespace(ConfigSections.MAP, "payments"), new ConfigNamespace(ConfigSections.QUEUE, "jobs"));
        assertThat(result.getIgnoredConfigs()).containsExactly(new ConfigNamespace(ConfigSections.MAP, "orders"));
        assertThat(member.getConfig().getMapConfig("payments").getBackupCount()).isEqualTo(2);
        assertThat(member.getConfig().getMapConfig("orders").getBackupCount()).isEqualTo(1);
        assertThat(member.getConfig().getQueueConfig("jobs").getMaxSize()).isEqualTo(10);
    }

    @Test
    public void updateAddsTheConfigurationsOfAPatch() {
        HecatonInstance first = factory.newHecatonInstance(smallInstanceConfig());
        HecatonInstance second = factory.newHecatonInstance(smallInstanceConfig());
        assertClusterSizeEventually(2, first, second);

        getNodeEngineImpl(first).getConfigurationService().updateAsync("""
                hecaton:
                  map:
                    patched:
                      backup-count: 2
                """);

        assertTrueEventually(() -> assertThat(second.getConfig().getMapConfigOrNull("patched")).isNotNull());
        assertThat(second.getConfig().getMapConfig("patched").getBackupCount()).isEqualTo(2);
    }
}
