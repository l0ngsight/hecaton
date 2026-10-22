/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.config.ConfigNamespace;
import uk.org.hecaton.internal.config.ConfigSections;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static uk.org.hecaton.test.Accessors.getService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Configuration reload (F16): a member configured from a file adds the new configurations of the
 * file. Hazelcast's open-source edition refused the reload (the former ConfigUpdateWithoutEnterpriseTest).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigReloadFromFileTest extends HecatonTestSupport {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void reloadAddsTheNewConfigurationsOfTheFile() throws IOException {
        File configFile = temporaryFolder.newFile(randomName() + ".xml");
        write(configFile, "");
        HecatonInstance member = createHecatonInstance(Config.loadFromFile(configFile));
        ConfigurationService configurationService = getService(member, ConfigurationService.SERVICE_NAME);

        write(configFile, "<map name=\"reloaded\"><backup-count>2</backup-count></map>");
        ConfigUpdateResult result = configurationService.update();

        assertThat(result.getAddedConfigs()).contains(new ConfigNamespace(ConfigSections.MAP, "reloaded"));
        assertThat(member.getConfig().getMapConfig("reloaded").getBackupCount()).isEqualTo(2);
    }

    @Test
    public void reloadNeedsAConfigurationFile() {
        ConfigurationService configurationService = getService(createHecatonInstance(), ConfigurationService.SERVICE_NAME);
        assertThatThrownBy(configurationService::update)
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("not configured from a file");
    }

    private static void write(File file, String content) throws IOException {
        Files.writeString(file.toPath(), "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\">" + content
                + "</hecaton>\n");
    }
}
