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

package uk.org.hecaton.internal.ascii;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.RestApiConfig;
import uk.org.hecaton.config.RestEndpointGroup;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestAwareInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.logging.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class RestLogLevelTest {

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @BeforeClass
    public static void beforeClass() {
        Hecaton.shutdownAll();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config createConfig() {
        return HecatonTestSupport.smallInstanceConfig();
    }

    protected Config createReadWriteConfig() {
        Config config = createConfig();
        RestApiConfig restApiConfig =
                new RestApiConfig().setEnabled(true).enableGroups(RestEndpointGroup.CLUSTER_READ).enableGroups(
                        RestEndpointGroup.CLUSTER_WRITE);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected Config createReadOnlyConfig() {
        Config config = createConfig();
        RestApiConfig restApiConfig =
                new RestApiConfig().setEnabled(true).enableGroups(RestEndpointGroup.CLUSTER_READ).disableGroups(
                        RestEndpointGroup.CLUSTER_WRITE);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected Config createWriteOnlyConfig() {
        Config config = createConfig();
        RestApiConfig restApiConfig =
                new RestApiConfig().setEnabled(true).disableGroups(RestEndpointGroup.CLUSTER_READ).enableGroups(
                        RestEndpointGroup.CLUSTER_WRITE);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected String getPassword() {
        return "";
    }

    @Test
    public void testDisabledRest() {
        // REST should be disabled by default
        HecatonInstance instance = factory.newHecatonInstance(createConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertThrows(IOException.class, communicator::getLogLevel);
    }

    @Test
    public void testEnabledRestButDisabledGroupClusterRead() {
        HecatonInstance instance = factory.newHecatonInstance(createWriteOnlyConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertThrows(IOException.class, communicator::getLogLevel);
    }

    @Test
    public void testEnabledRestButDisabledGroupClusterWrite() {
        Config config = createReadOnlyConfig();
        HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertThrows(IOException.class, () -> communicator.setLogLevel(config.getClusterName(), getPassword(), Level.FINE));
    }

    @Test
    public void testGetSetResetLogLevel() throws IOException {
        Config config = createReadWriteConfig();
        HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        HTTPCommunicator.ConnectionResponse response = communicator.getLogLevel();
        JsonValue jsonValue = Json.parse(response.response).asObject().get("logLevel");
        assertTrue(jsonValue.isNull());

        response = communicator.setLogLevel(config.getClusterName(), getPassword(), Level.FINE);
        jsonValue = Json.parse(response.response).asObject().get("message");
        assertEquals("log level is changed", jsonValue.asString());

        response = communicator.getLogLevel();
        jsonValue = Json.parse(response.response).asObject().get("logLevel");
        assertEquals(Level.FINE.getName(), jsonValue.asString());

        response = communicator.resetLogLevel(config.getClusterName(), getPassword());
        jsonValue = Json.parse(response.response).asObject().get("message");
        assertEquals("log level is reset", jsonValue.asString());

        response = communicator.getLogLevel();
        jsonValue = Json.parse(response.response).asObject().get("logLevel");
        assertTrue(jsonValue.isNull());
    }

}
