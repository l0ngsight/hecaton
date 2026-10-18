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

package uk.org.hecaton.internal.management;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getHecatonInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TimedMemberStateIntegrationTest extends HecatonTestSupport {

    @Test
    public void testServices() {
        HecatonInstance hz = createHecatonInstance(smallInstanceConfig());
        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHecatonInstanceImpl(hz));

        hz.getMap("trial").put(1, 1);
        hz.getMultiMap("trial").put(2, 2);
        hz.getQueue("trial").offer(3);
        hz.getTopic("trial").publish("Hello");
        hz.getReliableTopic("trial").publish("Hello");
        hz.getReplicatedMap("trial").put(3, 3);
        hz.getExecutorService("trial");

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        assertEquals("dev", timedMemberState.clusterName);
    }

    @Test
    public void testSSL_defaultConfig() {
        HecatonInstance hz = createHecatonInstance(smallInstanceConfig());
        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHecatonInstanceImpl(hz));

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        assertFalse(timedMemberState.sslEnabled);
    }

    @Test
    public void testSSL_enabled() {
        testSSL(true);
    }

    @Test
    public void testSSL_disabled() {
        testSSL(false);
    }

    private void testSSL(boolean enabled) {
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(enabled);

        Config config = smallInstanceConfig();
        config.getNetworkConfig().setSSLConfig(sslConfig);

        HecatonInstance hz = createHecatonInstance(config);
        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHecatonInstanceImpl(hz));

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        assertEquals(enabled, timedMemberState.sslEnabled);
    }

    @Test
    public void testScripting_defaultValue() {
        testScripting(null);
    }

    @Test
    public void testScripting_enabled() {
        testScripting(true);
    }

    @Test
    public void testScripting_disabled() {
        testScripting(false);
    }

    private void testScripting(Boolean enabled) {
        Config config = smallInstanceConfig();
        if (enabled != null) {
            config.getManagementCenterConfig().setScriptingEnabled(enabled);
        }

        HecatonInstance hz = createHecatonInstance(config);
        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHecatonInstanceImpl(hz));

        TimedMemberState timedMemberState = factory.createTimedMemberState();
        boolean expected = enabled == null ? false : enabled;
        assertEquals(expected, timedMemberState.scriptingEnabled);
    }
}
