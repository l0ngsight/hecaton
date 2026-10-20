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

package uk.org.hecaton.spi.discovery.multicast;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.config.properties.ValidationException;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;

import static uk.org.hecaton.test.OverridePropertyRule.clear;
import static uk.org.hecaton.test.OverridePropertyRule.set;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class MemberToMemberDiscoveryTest extends HecatonTestSupport {

    @Rule
    public final OverridePropertyRule overrideJoinWaitSecondsRule = set("hecaton.wait.seconds.before.join", "10");
    @Rule
    public final OverridePropertyRule overrideMergeFirstRunDelayRule = set("hecaton.merge.first.run.delay.seconds", "5");
    @Rule
    public final OverridePropertyRule overrideMergeNextRunDelayRule = set("hecaton.merge.next.run.delay.seconds", "5");
    @Rule
    public final OverridePropertyRule overridePreferIpv4Rule = set("java.net.preferIPv4Stack", "true");
    @Rule
    public final OverridePropertyRule overrideHecatonLocalAddressRule = clear("hecaton.local.localAddress");

    private Config config;

    @Before
    public void setUp() {
        String xmlFileName = "hecaton-multicast-plugin.xml";
        InputStream xmlResource = MulticastPropertiesTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        config = new XmlConfigBuilder(xmlResource).build();
    }

    @After
    public void tearDown() {
        Hecaton.shutdownAll();
    }

    @Test
    public void formClusterWithTwoMembersTest() {
        HecatonInstance instance = Hecaton.newHecatonInstance(config);
        Hecaton.newHecatonInstance(config);
        assertClusterSizeEventually(2, instance);
    }

    @Test(expected = ValidationException.class)
    public void invalidPortPropertyTest() {
        String xmlFileName = "hecaton-multicast-plugin-invalid-port.xml";
        InputStream xmlResource = MulticastDiscoveryStrategy.class.getClassLoader().getResourceAsStream(xmlFileName);
        config = new XmlConfigBuilder(xmlResource).build();

        Hecaton.newHecatonInstance(config);
    }
}
