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

package uk.org.hecaton.client.config;

import uk.org.hecaton.config.AbstractConfigImportVariableReplacementTest.IdentityReplacer;
import uk.org.hecaton.config.AbstractConfigImportVariableReplacementTest.TestReplacer;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.helpers.DeclarativeConfigFileHelper;
import uk.org.hecaton.config.replacer.EncryptionReplacer;
import uk.org.hecaton.config.test.builders.ConfigReplacerBuilder;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static uk.org.hecaton.client.config.XmlClientConfigBuilderTest.HECATON_CLIENT_END_TAG;
import static uk.org.hecaton.client.config.XmlClientConfigBuilderTest.HECATON_CLIENT_START_TAG;
import static uk.org.hecaton.client.config.XmlClientConfigBuilderTest.buildConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientConfigImportVariableReplacementTest extends AbstractClientConfigImportVariableReplacementTest {

    private DeclarativeConfigFileHelper helper;

    @Before
    public void setUp() {
        helper = new DeclarativeConfigFileHelper();
    }

    @After
    public void tearDown() {
        helper.ensureTestConfigDeleted();
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportElementOnlyAppearsInTopLevel() {
        String xml = HECATON_CLIENT_START_TAG
                + "   <network>\n"
                + "        <import resource=\"\"/>\n"
                + "   </network>\n"
                + HECATON_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHecatonElementOnlyAppearsOnce() {
        String xml = HECATON_CLIENT_START_TAG
                + "   <hecaton-client>"
                + "   </hecaton-client>"
                + HECATON_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Test
    @Override
    public void testImportResourceWithConfigReplacers() throws IOException {
        String configReplacer = HECATON_CLIENT_START_TAG
            + "    <config-replacers>\n"
            + "        <replacer class-name='" + IdentityReplacer.class.getName() + "'/>\n"
            + "    </config-replacers>\n"
            + HECATON_CLIENT_END_TAG;
        String configLocation = helper.givenConfigFileInWorkDir("config-replacers.xml", configReplacer).getAbsolutePath();

        String xml = HECATON_CLIENT_START_TAG
            + "    <import resource=\"${config.location}\"/>\n"
            + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
            + HECATON_CLIENT_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.setProperty("config.location", configLocation);
        ClientConfig groupConfig = buildConfig(xml, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Test
    @Override
    public void testImportResourceWithNestedImports() throws IOException {
        String configReplacer = HECATON_CLIENT_START_TAG
            + "    <config-replacers>\n"
            + "        <replacer class-name='" + IdentityReplacer.class.getName() + "'/>\n"
            + "    </config-replacers>\n"
            + HECATON_CLIENT_END_TAG;
        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacers.xml", configReplacer).getAbsolutePath();

        String clusterName = HECATON_CLIENT_START_TAG
            + "    <import resource=\"" + "file:///" + configReplacerLocation + "\"/>\n"
            + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
            + HECATON_CLIENT_END_TAG;

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.xml", clusterName).getAbsolutePath();

        String xml = HECATON_CLIENT_START_TAG
            + "    <import resource=\"${config.location}\"/>\n"
            + HECATON_CLIENT_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.setProperty("config.location", clusterNameLocation);
        ClientConfig groupConfig = buildConfig(xml, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Test
    @Override
    public void testImportResourceWithNestedImportsAndProperties() throws IOException {
        ConfigReplacerBuilder testReplacer = new ConfigReplacerBuilder()
            .withClass(TestReplacer.class)
            .addProperty("p1", "${p1}")
            .addProperty("p2", "")
            .addProperty("p3", "another property")
            .addProperty("p4", "&lt;test/&gt;");
        String configReplacer = HECATON_CLIENT_START_TAG
            + "    <config-replacers fail-if-value-missing='false'>\n"
            + testReplacer.build()
            + "    </config-replacers>\n"
            + HECATON_CLIENT_END_TAG;
        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.xml", configReplacer).getAbsolutePath();

        String clusterName = HECATON_CLIENT_START_TAG
            + "    <import resource=\"" + "file:///" + configReplacerLocation + "\"/>\n"
            + "    <cluster-name>$T{p1} $T{p2} $T{p3} $T{p4} $T{p5}</cluster-name>\n"
            + HECATON_CLIENT_END_TAG;

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.xml", clusterName).getAbsolutePath();

        String xml = HECATON_CLIENT_START_TAG
            + "    <import resource=\"${config.location}\"/>\n"
            + HECATON_CLIENT_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.setProperty("config.location", clusterNameLocation);
        properties.setProperty("p1", "a property");
        ClientConfig config = buildConfig(xml, properties);
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    @Override
    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
        String networkConfig = HECATON_CLIENT_START_TAG
                + "<network>"
                + "    <cluster-members>"
                + "      <address>192.168.100.100</address>"
                + "      <address>127.0.0.10</address>"
                + "    </cluster-members>"
                + "    <cluster-routing mode=\"MULTI_MEMBER\" routing-strategy=\"PARTITION_GROUPS\"/>"
                + "    <redo-operation>true</redo-operation>"
                + "    <socket-interceptor enabled=\"true\">"
                + "      <class-name>uk.org.hecaton.examples.MySocketInterceptor</class-name>"
                + "      <properties>"
                + "        <property name=\"foo\">bar</property>"
                + "      </properties>"
                + "    </socket-interceptor>"
                + "  </network>"
                + HECATON_CLIENT_END_TAG;
        String configLocationPath = helper.givenConfigFileInWorkDir("config-network.xml", networkConfig).getAbsolutePath();

        String xml = HECATON_CLIENT_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HECATON_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml, "config.location", configLocationPath);
        assertEquals(RoutingMode.MULTI_MEMBER, config.getNetworkConfig().getClusterRoutingConfig().getRoutingMode());
        assertEquals(ClusterRoutingConfig.DEFAULT_ROUTING_STRATEGY,
                config.getNetworkConfig().getClusterRoutingConfig().getRoutingStrategy());
        assertTrue(config.getNetworkConfig().isRedoOperation());
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.100.100");
        assertContains(config.getNetworkConfig().getAddresses(), "127.0.0.10");
    }

    @Override
    @Test
    public void testImportedConfigVariableReplacement() throws IOException {
        String networkConfig = HECATON_CLIENT_START_TAG
                + "  <network>"
                + "    <cluster-members>"
                + "      <address>${ip.address}</address>"
                + "    </cluster-members>"
                + "  </network>"
                + HECATON_CLIENT_END_TAG;
        String configLocationPath = helper.givenConfigFileInWorkDir("config-network.xml", networkConfig).getAbsolutePath();

        String xml = HECATON_CLIENT_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HECATON_CLIENT_END_TAG;

        Properties properties = new Properties();
        properties.setProperty("config.location", configLocationPath);
        properties.setProperty("ip.address", "192.168.5.5");
        ClientConfig config = buildConfig(xml, properties);
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.5.5");
    }

    private String xmlContentWithImportResource(String url) {
        return HECATON_CLIENT_START_TAG
            + "    <import resource=\"" + url + "\"/>\n"
            + HECATON_CLIENT_END_TAG;
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        String xmlWithCyclicImport = helper.createFilesWithCycleImports(
            this::xmlContentWithImportResource,
            helper.givenConfigFileInWorkDir("hz1.xml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz2.xml", "").getAbsolutePath()
        );

        buildConfig(xmlWithCyclicImport);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        String xmlWithCyclicImport = helper.createFilesWithCycleImports(
            this::xmlContentWithImportResource,
            helper.givenConfigFileInWorkDir("hz1.xml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz2.xml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz3.xml", "").getAbsolutePath()
        );

        buildConfig(xmlWithCyclicImport);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        String pathToEmptyFile = createEmptyFile();
        String configXml = HECATON_CLIENT_START_TAG
                + "    <import resource=\"file:///" + pathToEmptyFile + "\"/>\n"
                + HECATON_CLIENT_END_TAG;
        buildConfig(configXml);
    }

    private String createEmptyFile() throws Exception {
        return helper.givenConfigFileInWorkDir("hz1.xml", "").getAbsolutePath();
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceThrowsException() {
        String xml = HECATON_CLIENT_START_TAG
                + "    <import resource=\"\"/>\n"
                + HECATON_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportNotExistingResourceThrowsException() {
        String xml = HECATON_CLIENT_START_TAG
                + "    <import resource=\"notexisting.xml\"/>\n"
                + HECATON_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = HecatonException.class)
    public void testImportNotExistingUrlResourceThrowsException() {
        String xml = HECATON_CLIENT_START_TAG
                + "    <import resource=\"file:///notexisting.xml\"/>\n"
                + HECATON_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test
    public void testReplacers() throws Exception {
        String pathToFileWithPassword = helper.givenConfigFileInWorkDir(
            getClass().getSimpleName() + ".pwd",
            "This is a password"
        ).getAbsolutePath();

        ConfigReplacerBuilder encryptionReplacer = new ConfigReplacerBuilder()
            .withClass(EncryptionReplacer.class)
            .addProperty("passwordFile", pathToFileWithPassword)
            .addProperty("passwordUserProperties", false)
            .addProperty("keyLengthBits", 64)
            .addProperty("saltLengthBytes", 8)
            .addProperty("cipherAlgorithm", "DES")
            .addProperty("secretKeyFactoryAlgorithm", "PBKDF2WithHmacSHA1")
            .addProperty("secretKeyAlgorithm", "DES");
        ConfigReplacerBuilder identityReplacer = new ConfigReplacerBuilder()
            .withClass(IdentityReplacer.class);

        String xml = HECATON_CLIENT_START_TAG
                + "    <config-replacers>\n"
                + encryptionReplacer.build()
                + identityReplacer.build()
                + "    </config-replacers>\n"
                + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
                + "    <instance-name>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</instance-name>\n"
                + HECATON_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml, System.getProperties());
        assertEquals(System.getProperty("java.version") + " dev", config.getClusterName());
        assertEquals("My very secret secret", config.getInstanceName());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testMissingReplacement() {
        String xml = HECATON_CLIENT_START_TAG
                + "    <config-replacers>\n"
                + "        <replacer class-name='" + EncryptionReplacer.class.getName() + "'/>\n"
                + "    </config-replacers>\n"
                + "    <cluster-name>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</cluster-name>\n"
                + HECATON_CLIENT_END_TAG;
        buildConfig(xml, System.getProperties());
    }

    @Override
    @Test
    public void testReplacerProperties() {
        ConfigReplacerBuilder testReplacer = new ConfigReplacerBuilder()
            .withClass(TestReplacer.class)
            .addProperty("p1", "a property")
            .addProperty("p2", "")
            .addProperty("p3", "another property")
            .addProperty("p4", "&lt;test/&gt;");

        String xml = HECATON_CLIENT_START_TAG
                + "    <config-replacers fail-if-value-missing='false'>\n"
                + testReplacer.build()
                + "    </config-replacers>\n"
                + "    <cluster-name>$T{p1} $T{p2} $T{p3} $T{p4} $T{p5}</cluster-name>\n"
                + HECATON_CLIENT_END_TAG;
        ClientConfig clientConfig = buildConfig(xml, System.getProperties());
        assertEquals("a property  another property <test/> $T{p5}", clientConfig.getClusterName());
    }


    /**
     * Given: No replacer is used in the configuration file<br>
     * When: A property variable is used within the file<br>
     * Then: The configuration parsing doesn't fail and the variable string remains unchanged (i.e. backward compatible
     * behavior, as if {@code fail-if-value-missing} attribute is {@code false}).
     */
    @Override
    @Test
    public void testNoConfigReplacersMissingProperties() {
        String xml = HECATON_CLIENT_START_TAG
                + "    <cluster-name>${noSuchPropertyAvailable}</cluster-name>\n"
                + HECATON_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml, System.getProperties());
        assertEquals("${noSuchPropertyAvailable}", config.getClusterName());
    }

    @Override
    @Test
    public void testImportConfigFromClassPath() {
        String xml = HECATON_CLIENT_START_TAG
                + "    <import resource=\"classpath:hecaton-client-c1.xml\"/>\n"
                + HECATON_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);
        assertEquals("cluster1", config.getClusterName());
    }

    @Override
    @Test
    public void testReplaceVariablesUseSystemProperties() {
        String xml = HECATON_CLIENT_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop\">${variable}</property>\n"
                + "    </properties>\n"
                + HECATON_CLIENT_END_TAG;

        System.setProperty("variable", "foobar");
        ClientConfig config = buildConfig(xml);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithClasspathConfig() {
        System.setProperty("variable", "foobar");
        ClientConfig config = new ClientClasspathXmlConfig("test-hecaton-client-variable.xml");

        assertEquals("foobar", config.getProperty("prop"));
    }
}
