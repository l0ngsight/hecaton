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

import static uk.org.hecaton.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static uk.org.hecaton.test.Accessors.getService;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigUpdateWithoutEnterpriseTest extends HecatonTestSupport {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test(expected = UnsupportedOperationException.class)
    public void reloadWithoutEnterprise() throws IOException {
        String configAsString = "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\"></hecaton>\n";
        File configFile = temporaryFolder.newFile(randomName() + ".xml");
        Files.write(configFile.toPath(), configAsString.getBytes());
        System.setProperty(SYSPROP_MEMBER_CONFIG, configFile.getAbsolutePath());

        ConfigurationService configurationService = getService(createHecatonInstance(), ConfigurationService.SERVICE_NAME);
        configurationService.update();
    }
}
