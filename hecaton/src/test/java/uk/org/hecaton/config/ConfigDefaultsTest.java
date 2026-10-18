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

import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigDefaultsTest extends HecatonTestSupport {

    private static final Config JAVA_CONFIG = javaConfig();
    private static final Config DEFAULT_XML_CONFIG = defaultXmlConfig();
    private static final Config EMPTY_XML_CONFIG = emptyXmlConfig();

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"Java - hecaton-default.xml", JAVA_CONFIG, DEFAULT_XML_CONFIG},
                {"Java - empty XML", JAVA_CONFIG, EMPTY_XML_CONFIG},
                {"hecaton-default.xml - empty XML", DEFAULT_XML_CONFIG, EMPTY_XML_CONFIG},
        });
    }

    @Parameter
    public String name;

    @Parameter(1)
    public Config c1;

    @Parameter(2)
    public Config c2;

    @Test
    public void testCompatibility() {
        ConfigCompatibilityChecker.isCompatible(c1, c2);
    }

    private static Config javaConfig() {
        return new Config();
    }

    private static Config defaultXmlConfig() {
        InputStream inputStream = ConfigDefaultsTest.class.getClassLoader().getResourceAsStream("hecaton-default.xml");
        return new XmlConfigBuilder(inputStream).build();
    }

    private static Config emptyXmlConfig() {
        String xml = "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\">\n</hecaton>\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(xml.getBytes());
        return new XmlConfigBuilder(inputStream).build();
    }
}
