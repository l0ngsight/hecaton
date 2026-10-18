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

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

import static uk.org.hecaton.config.XMLConfigBuilderTest.HECATON_END_TAG;
import static uk.org.hecaton.config.XMLConfigBuilderTest.HECATON_START_TAG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class XmlSchemaValidationTest {

    @Test
    public void testXmlDeniesDuplicateClusterNameConfig() {
        expectDuplicateElementError("cluster-name", () -> {
            String clusterName = "<cluster-name>foobar</cluster-name>";
            buildConfig(HECATON_START_TAG + clusterName + clusterName + HECATON_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateNetworkConfig() {
        expectDuplicateElementError("network", () -> {
            String networkConfig = """
                        <network>
                            <join>
                                <multicast enabled="false"/>
                                <tcp-ip enabled="true"/>
                            </join>
                        </network>
                    """;
            buildConfig(HECATON_START_TAG + networkConfig + networkConfig + HECATON_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateLicenseKeyConfig() {
        expectDuplicateElementError("license-key", () -> {
            String licenseConfig = "    <license-key>foo</license-key>";
            buildConfig(HECATON_START_TAG + licenseConfig + licenseConfig + HECATON_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicatePropertiesConfig() {
        expectDuplicateElementError("properties", () -> {
            String propertiesConfig = """
                        <properties>
                            <property name='foo'>fooval</property>
                        </properties>
                    """;
            buildConfig(HECATON_START_TAG + propertiesConfig + propertiesConfig + HECATON_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicatePartitionGroupConfig() {
        expectDuplicateElementError("partition-group", () -> {
            String partitionConfig = """
                       <partition-group>
                          <member-group>
                              <interface>foo</interface>
                          </member-group>
                       </partition-group>
                    """;
            buildConfig(HECATON_START_TAG + partitionConfig + partitionConfig + HECATON_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateListenersConfig() {
        expectDuplicateElementError("listeners", () -> {
            String listenersConfig = """
                       <listeners>
                            <listener>foo</listener>

                       </listeners>
                    """;
            buildConfig(HECATON_START_TAG + listenersConfig + listenersConfig + HECATON_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateSerializationConfig() {
        expectDuplicateElementError("serialization", () -> {
            String serializationConfig = """
                           <serialization>
                            <portable-version>0</portable-version>
                            <data-serializable-factories>
                                <data-serializable-factory factory-id="1">uk.org.hecaton.examples.DataSerializableFactory
                                </data-serializable-factory>
                            </data-serializable-factories>
                            <portable-factories>
                                <portable-factory factory-id="1">uk.org.hecaton.examples.PortableFactory</portable-factory>
                            </portable-factories>
                            <serializers>
                                <global-serializer>uk.org.hecaton.examples.GlobalSerializerFactory</global-serializer>
                                <serializer type-class="uk.org.hecaton.examples.DummyType"
                                    class-name="uk.org.hecaton.examples.SerializerFactory"/>
                            </serializers>
                            <check-class-def-errors>true</check-class-def-errors>
                        </serialization>
                    """;
            buildConfig(HECATON_START_TAG + serializationConfig + serializationConfig + HECATON_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateSecurityConfig() {
        expectDuplicateElementError("security", () -> {
            String securityConfig = "   <security/>\n";
            buildConfig(HECATON_START_TAG + securityConfig + securityConfig + HECATON_END_TAG);
        });
    }

    @Test
    public void testXmlDeniesDuplicateMemberAttributesConfig() {
        expectDuplicateElementError("member-attributes", () -> {
            String memberAttConfig = """
                        <member-attributes>
                            <attribute name="attribute">1234.5678</attribute>
                        </member-attributes>
                    """;
            buildConfig(HECATON_START_TAG + memberAttConfig + memberAttConfig + HECATON_END_TAG);
        });
    }

    private void expectDuplicateElementError(String elName, ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining(elName);
    }

    private static Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

}
