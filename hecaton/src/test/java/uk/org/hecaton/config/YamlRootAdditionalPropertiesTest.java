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

package uk.org.hecaton.config;

import uk.org.hecaton.client.config.YamlClientConfigBuilderTest;
import uk.org.hecaton.internal.config.SchemaViolationConfigurationException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.YamlConfigBuilderTest.buildConfig;
import static uk.org.hecaton.test.OverridePropertyRule.clear;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class YamlRootAdditionalPropertiesTest {

    @Rule
    public OverridePropertyRule indentationCheckEnabled = clear("hecaton.yaml.config.indentation.check.enabled");

    @Test
    public void testMisIndentedMemberConfigProperty_failsValidation() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> buildConfig("""
                        hecaton:
                          instance-name: 'my-instance'
                        cluster-name: 'my-cluster'
                        """)
        );
        assertEquals(format("Mis-indented hecaton configuration property found: [cluster-name]%n"
                + "Note: you can disable this validation by passing the "
                + "-Dhecaton.yaml.config.indentation.check.enabled=false system property"), actual.getMessage());
    }

    @Test
    public void misIndentedRootProperty_validationDisabled() {
        indentationCheckEnabled.setOrClearProperty("false");
        buildConfig("""
                hecaton:
                  instance-name: 'my-instance'
                cluster-name: 'my-cluster'
                """);
    }

    @Test
    public void testMisIndented_NonConfigProperty_passes() {
        buildConfig("""
                hecaton:
                  instance-name: 'my-instance'
                other-prop: ''
                """);
    }

    @Test
    public void testMisIndented_ClientConfigProperty_failsValidation() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> YamlClientConfigBuilderTest.buildConfig("""
                        hecaton-client:
                          instance-name: 'my-instance'
                        client-labels: 'my-lbl'
                        """)
        );
        assertEquals(format("Mis-indented hecaton configuration property found: [client-labels]%n"
                + "Note: you can disable this validation by passing the "
                + "-Dhecaton.yaml.config.indentation.check.enabled=false system property"), actual.getMessage());
    }

    @Test
    public void multipleMisIndented_configProps() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> YamlClientConfigBuilderTest.buildConfig("""
                        hecaton-client: {}
                        instance-name: 'my-instance'
                        client-labels: 'my-lbl'
                        """)
        );
        assertEquals(new SchemaViolationConfigurationException(format("2 schema violations found%n"
                + "Note: you can disable this validation by passing the "
                + "-Dhecaton.yaml.config.indentation.check.enabled=false system property"), "#", "#", asList(
                new SchemaViolationConfigurationException("Mis-indented hecaton configuration property found: [instance-name]",
                        "#", "#", emptyList()),
                new SchemaViolationConfigurationException("Mis-indented hecaton configuration property found: [client-labels]",
                        "#", "#", emptyList())
        )), actual);
    }
}
