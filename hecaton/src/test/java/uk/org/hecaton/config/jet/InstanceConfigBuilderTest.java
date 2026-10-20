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

package uk.org.hecaton.config.jet;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.config.YamlConfigBuilder;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InstanceConfigBuilderTest {

    @Parameterized.Parameter
    public String extension;

    @Parameterized.Parameters(name = "extension:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList("xml", "yaml");
    }

    @Test
    public void whenFieldsConfiguredInJetConfig_thenInstanceConfigIgnored() {
        Config config = getConfig("jet-config-and-instance-config");

        assertEquals(4, config.getJetConfig().getBackupCount());
        assertEquals(4, config.getJetConfig().getInstanceConfig().getBackupCount());
    }

    @Test
    public void whenFieldsNotConfiguredInJetConfig_thenInstanceConfigNotIgnored() {
        Config config = getConfig("just-instance-config");

        assertEquals(5, config.getJetConfig().getBackupCount());
        assertEquals(5, config.getJetConfig().getInstanceConfig().getBackupCount());
    }

    private Config getConfig(String fileName) {
        InputStream inputStream = getClass().getResourceAsStream(fileName + "." + extension);
        if (extension.equals("xml")) {
            return new XmlConfigBuilder(inputStream).build();
        } else {
            return new YamlConfigBuilder(inputStream).build();
        }
    }
}
