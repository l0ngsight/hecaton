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
import uk.org.hecaton.config.ConfigXmlGenerator;
import uk.org.hecaton.config.InMemoryXmlConfig;
import uk.org.hecaton.internal.cluster.Versions;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.dynamicconfig.DynamicConfigXmlGenerator.licenseKeyXmlGenerator;
import static org.junit.Assert.assertEquals;

// Please also take a look at the ConfigXmlGeneratorTest.
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigXmlGeneratorTest extends AbstractDynamicConfigGeneratorTest {

    private static final ILogger LOGGER = Logger.getLogger(DynamicConfigXmlGeneratorTest.class);

    // LICENSE KEY

    @Test
    public void testLicenseKey() {
        String licenseKey = randomString();
        Config config = new Config().setLicenseKey(licenseKey);

        StringBuilder xmlBuilder = new StringBuilder();
        ConfigXmlGenerator.XmlGenerator gen = new ConfigXmlGenerator.XmlGenerator(xmlBuilder);

        // generate string
        xmlBuilder.append("<hecaton ")
                .append("xmlns=\"https://hecaton.org.uk/schema/config\"\n")
                .append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
                .append("xsi:schemaLocation=\"https://hecaton.org.uk/schema/config ")
                .append("https://hecaton.org.uk/schema/config/hecaton-config-")
                .append(Versions.CURRENT_CLUSTER_VERSION.toString())
                .append(".xsd\">");
        licenseKeyXmlGenerator(gen, config);
        xmlBuilder.append("</hecaton>");

        // recreate the config object
        String xmlString = xmlBuilder.toString();
        Config decConfig = new InMemoryXmlConfig(xmlString);

        // test
        String actualLicenseKey = decConfig.getLicenseKey();
        assertEquals(config.getLicenseKey(), actualLicenseKey);
    }

    @Override
    protected Config getNewConfigViaGenerator(Config config) {
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator(true, true);
        String xml = configXmlGenerator.generate(config);
        LOGGER.fine("\n%s", xml);
        return new InMemoryXmlConfig(xml);

    }
}
