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
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.dataconnection.HecatonDataConnection;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class HecatonDataConnectionConfigValidatorTest {

    @Test
    public void testValidateNone() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();

        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HecatonException.class);
    }

    @Test
    public void testValidateEitherStringOrFilePath() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML_PATH, "xml_path");
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML, "xml");

        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HecatonException.class);
    }

    @Test
    public void testValidateBothFilePathsSet() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML_PATH, "xml_path");
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_YML_PATH, "yaml_path");

        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HecatonException.class);
    }

    @Test
    public void testValidateBothStringsSet() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML, "xml");
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_YML, "yaml");

        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HecatonException.class);
    }

    @Test
    public void testValidateOnlyFilePath() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML_PATH, "xml_path");

        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();

        assertThatCode(() -> validator.validate(dataConnectionConfig))
                .doesNotThrowAnyException();
    }

    @Test
    public void testValidateEmptyFilePath() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML_PATH, "");

        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();

        assertThatCode(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HecatonException.class);
    }

    @Test
    public void testValidateOnlyString() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML, "xml");

        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();

        assertThatCode(() -> validator.validate(dataConnectionConfig))
                .doesNotThrowAnyException();
    }

    @Test
    public void testValidateEmptyString() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML, "");

        HecatonDataConnectionConfigValidator validator = new HecatonDataConnectionConfigValidator();

        assertThatCode(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HecatonException.class);
    }
}
