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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManagementCenterConfigTest extends HecatonTestSupport {

    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

    @Test
    public void testDefaultConfig() {
        HecatonInstance hz = factory.newHecatonInstance();
        checkConfig(hz);
    }

    @Test
    public void testProgrammaticConfig() {
        HecatonInstance hz = factory.newHecatonInstance(new Config());
        checkConfig(hz);
    }

    @Test
    public void testXmlConfig() {
        InMemoryXmlConfig xmlConfig = new InMemoryXmlConfig("""
                <hecaton xmlns="https://hecaton.org.uk/schema/config">
                <management-center />
                </hecaton>""");
        HecatonInstance hz = factory.newHecatonInstance(xmlConfig);
        checkConfig(hz);
    }

    @Test
    public void testYamlConfig() {
        InMemoryYamlConfig yamlConfig = new InMemoryYamlConfig("hecaton:\n  cluster-name: name\n");
        HecatonInstance hz = factory.newHecatonInstance(yamlConfig);
        checkConfig(hz);
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(ManagementCenterConfig.class)
                      .suppress(Warning.NONFINAL_FIELDS)
                      .verify();
    }

    private void checkConfig(HecatonInstance hz) {
        assertFalse(hz.getConfig().getManagementCenterConfig().isScriptingEnabled());
        assertFalse(hz.getConfig().getManagementCenterConfig().isConsoleEnabled());
        assertTrue(hz.getConfig().getManagementCenterConfig().isDataAccessEnabled());
    }
}
