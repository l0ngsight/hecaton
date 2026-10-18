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

package uk.org.hecaton.dataconnection.impl.hecatondataconnection;

import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.dataconnection.HecatonDataConnection;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class HecatonDataConnectionConfigLoaderTest {

    @Test
    public void testNothingLoaded() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML, "xml");

        HecatonDataConnectionConfigLoader loader = new HecatonDataConnectionConfigLoader();
        DataConnectionConfig loadedConfig = loader.load(dataConnectionConfig);

        assertThat(loadedConfig.getProperty(HecatonDataConnection.CLIENT_XML)).isEqualTo("xml");
        assertNull(loadedConfig.getProperty(HecatonDataConnection.CLIENT_YML));
    }

    @Test
    public void testLoadXml() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        Path path = Paths.get("src", "test", "resources", "hecaton-client-test-external.xml");
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML_PATH, path.toString());

        HecatonDataConnectionConfigLoader loader = new HecatonDataConnectionConfigLoader();
        DataConnectionConfig loadedConfig = loader.load(dataConnectionConfig);

        assertNotNull(loadedConfig.getProperty(HecatonDataConnection.CLIENT_XML));
        assertNull(loadedConfig.getProperty(HecatonDataConnection.CLIENT_YML));
    }

    @Test
    public void testLoadYml() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        Path path = Paths.get("src", "test", "resources", "hecaton-client-test-external.yaml");
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_YML_PATH, path.toString());

        HecatonDataConnectionConfigLoader loader = new HecatonDataConnectionConfigLoader();
        DataConnectionConfig loadedConfig = loader.load(dataConnectionConfig);

        assertNotNull(loadedConfig.getProperty(HecatonDataConnection.CLIENT_YML));
        assertNull(loadedConfig.getProperty(HecatonDataConnection.CLIENT_XML));
    }
}

