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

import uk.org.hecaton.config.cp.CPMapConfig;
import uk.org.hecaton.config.cp.CPSubsystemConfig;
import uk.org.hecaton.internal.cluster.Versions;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.TypeInfo;
import org.w3c.dom.traversal.DocumentTraversal;
import org.w3c.dom.traversal.NodeFilter;
import org.w3c.dom.traversal.NodeIterator;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;

import static uk.org.hecaton.instance.BuildInfoProvider.HECATON_INTERNAL_OVERRIDE_VERSION;
import static uk.org.hecaton.internal.util.XmlUtil.getNsAwareDocumentBuilderFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.w3c.dom.TypeInfo.DERIVATION_RESTRICTION;

/**
 * Test cases specific only to XML based configuration. The cases not
 * XML specific should be added to {@link XMLConfigBuilderTest}.
 * <p>
 * This test class is expected to contain only <strong>extra</strong> test
 * cases over the ones defined in {@link XMLConfigBuilderTest} in order
 * to cover XML specific cases where XML configuration derives from the
 * YAML configuration to allow usage of XML-native constructs.
 *
 * @see XMLConfigBuilderTest
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class XmlOnlyConfigBuilderTest {

    private static final String HECATON_START_TAG = "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\">\n";
    private static final String HECATON_END_TAG = "</hecaton>\n";

    @Test(expected = InvalidConfigurationException.class)
    public void testMissingNamespace() {
        String xml = "<hecaton/>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidNamespace() {
        String xml = "<hecaton xmlns=\"http://foo.bar\"/>";
        buildConfig(xml);
    }

    @Test
    public void testValidNamespace() {
        String xml = HECATON_START_TAG + HECATON_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHecatonTagAppearsTwice() {
        String xml = HECATON_START_TAG + "<hecaton/>" + HECATON_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHecatonInstanceNameEmpty() {
        String xml = HECATON_START_TAG + "<instance-name></instance-name>" + HECATON_END_TAG;
        buildConfig(xml);
    }

    @Test
    public void testXsdVersion() {
        String origVersionOverride = System.getProperty(HECATON_INTERNAL_OVERRIDE_VERSION);
        assertXsdVersion("0.0", "0.0");
        assertXsdVersion("3.9", "3.9");
        assertXsdVersion("3.9-SNAPSHOT", "3.9");
        assertXsdVersion("3.9.1-SNAPSHOT", "3.9");
        assertXsdVersion("3.10", "3.10");
        assertXsdVersion("3.10-SNAPSHOT", "3.10");
        assertXsdVersion("3.10.1-SNAPSHOT", "3.10");
        assertXsdVersion("99.99.99", "99.99");
        assertXsdVersion("99.99.99-SNAPSHOT", "99.99");
        assertXsdVersion("99.99.99-Beta", "99.99");
        assertXsdVersion("99.99.99-Beta-SNAPSHOT", "99.99");
        if (origVersionOverride != null) {
            System.setProperty(HECATON_INTERNAL_OVERRIDE_VERSION, origVersionOverride);
        } else {
            System.clearProperty(HECATON_INTERNAL_OVERRIDE_VERSION);
        }
    }

    @Test
    public void testConfig2Xml2DefaultConfig() {
        testConfig2Xml2Config("hecaton-default.xml");
    }

    @Test
    public void testConfig2Xml2FullConfig() {
        testConfig2Xml2Config("hecaton-fullconfig.xml");
    }

    @Test
    public void testConfig2Xml2Config_withAdvancedNetworkConfig() {
        testConfig2Xml2Config("hecaton-fullconfig-advanced-network-config.xml");
    }

    @Test
    public void testXSDDefaultXML() throws Exception {
        testXSDConfigXML("hecaton-default.xml");
    }

    @Test
    public void testFullConfig() throws Exception {
        testXSDConfigXML("hecaton-fullconfig.xml");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_noExtractor() {
        String xml = HECATON_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute></attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HECATON_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_noExtractor_singleTag() {
        String xml = HECATON_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute/>\n"
                + "       </attributes>"
                + "   </map>"
                + HECATON_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testCacheConfig_withInvalidEvictionConfig_failsFast() {
        String xml = HECATON_START_TAG
                + "    <cache name=\"cache\">"
                + "        <eviction size=\"10000000\" max-size-policy=\"ENTRY_COUNT\" eviction-policy=\"INVALID\"/>"
                + "    </cache>"
                + HECATON_END_TAG;

        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String xml = "<hecaton-client>"
                + "<cluster-name>dev</cluster-name>"
                + "</hecaton-client>";
        buildConfig(xml);
    }

    @Test
    public void testAddWhitespaceToNonSpaceStrings() throws Exception {
        // parse the default config file
        InputStream xmlResource = XMLConfigBuilderTest.class.getClassLoader().getResourceAsStream("hecaton-default.xml");
        DocumentBuilder builder = getNsAwareDocumentBuilderFactory().newDocumentBuilder();
        Document doc = builder.parse(xmlResource);
        // validate to augment with type information
        Validator validator = getValidator();
        DOMResult result = new DOMResult();
        validator.validate(new DOMSource(doc), result);
        Document validated = (Document) result.getNode();
        // add whitespace to non-space-string nodes
        assertTrue("No whitespace added", addWhitespaceToNonSpaceStrings(validated));
        String xml = serialize(validated);
        buildConfig(xml);
    }

    @Test
    public void testCPMapConfig() {
        Config config = new ClasspathXmlConfig("hecaton-fullconfig.xml");
        assertNotNull(config);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        assertEquals(2, cpSubsystemConfig.getCpMapConfigs().size());
        CPMapConfig map1Expected = new CPMapConfig("map1", 50);
        CPMapConfig map1Actual = cpSubsystemConfig.findCPMapConfig(map1Expected.getName());
        assertNotNull(map1Actual);
        assertEquals(map1Expected, map1Actual);
        CPMapConfig map2Expected = new CPMapConfig("map2", 25).setPurgeEnabled(true);
        CPMapConfig map2Actual = cpSubsystemConfig.findCPMapConfig(map2Expected.getName());
        assertNotNull(map2Actual);
        assertEquals(map2Expected, map2Actual);
    }

    private static void assertXsdVersion(String buildVersion, String expectedXsdVersion) {
        System.setProperty(HECATON_INTERNAL_OVERRIDE_VERSION, buildVersion);
        assertEquals("Unexpected release version retrieved for build version " + buildVersion, expectedXsdVersion,
                new XmlConfigBuilder().getReleaseVersion());
    }

    private static void testConfig2Xml2Config(String fileName) {
        Config config = new ClasspathXmlConfig(fileName);

        String xml = new ConfigXmlGenerator(true, false).generate(config);
        Config config2 = new InMemoryXmlConfig(xml);

        assertTrue(ConfigCompatibilityChecker.isCompatible(config, config2));
    }

    private static Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    private static void testXSDConfigXML(String xmlFileName) throws Exception {
        InputStream xmlResource = XMLConfigBuilderTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Source source = new StreamSource(xmlResource);
        Validator validator = getValidator();

        try {
            validator.validate(source);
        } catch (SAXException ex) {
            fail(xmlFileName + " is not valid because: " + ex);
        }
    }

    private static Validator getValidator() throws SAXException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hecaton-config-"
                + Versions.CURRENT_CLUSTER_VERSION + ".xsd");
        Schema schema = factory.newSchema(schemaResource);
        return schema.newValidator();
    }

    private static String serialize(Node node) throws TransformerException {
        StringWriter result = new StringWriter();
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(new DOMSource(node), new StreamResult(result));
        return result.toString();
    }

    private static boolean addWhitespaceToNonSpaceStrings(Document doc) {
        NodeIterator nodeIterator = ((DocumentTraversal) doc).createNodeIterator(
                doc.getDocumentElement(), NodeFilter.SHOW_ELEMENT, null, true);
        boolean added = false;
        Node node;
        while ((node = nodeIterator.nextNode()) != null) {
            if (isNonSpaceString(node)) {
                addWhitespace(node);
                added = true;
            }
            NamedNodeMap attrs = node.getAttributes();
            for (int i = 0; i < attrs.getLength(); i++) {
                Node attr = attrs.item(i);
                if (isNonSpaceString(attr)) {
                    addWhitespace(attr);
                    added = true;
                }
            }
        }
        return added;
    }

    private static void addWhitespace(Node node) {
        node.setNodeValue(" \n " + node.getNodeValue() + " \n ");
    }

    private static boolean isNonSpaceString(Node node) {
        TypeInfo typeInfo;
        if (node.getNodeType() == Node.ELEMENT_NODE) {
            typeInfo = ((Element) node).getSchemaTypeInfo();
        } else if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
            typeInfo = ((Attr) node).getSchemaTypeInfo();
        } else {
            typeInfo = null;
        }
        return typeInfo != null && typeInfo.isDerivedFrom("https://hecaton.org.uk/schema/config",
                "non-space-string", DERIVATION_RESTRICTION);
    }
}
