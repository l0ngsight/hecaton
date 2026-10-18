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
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestAwareInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class RestInstanceTest {

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
        return new Config();
    }

    protected Config createConfigWithRestEnabled() {
        Config config = createConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableGroups(RestEndpointGroup.CLUSTER_READ);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected Config createConfigWithRestEnabledAndClusterReadDisabled() {
        Config config = createConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true)
                                                         .disableGroups(RestEndpointGroup.CLUSTER_READ)
                                                         .enableGroups(RestEndpointGroup.CLUSTER_WRITE);
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    @Test
    public void testDisabledRest() {
        // REST should be disabled by default
        HecatonInstance instance = factory.newHecatonInstance(createConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        try {
            communicator.getInstanceInfo();
            fail("Rest is disabled. Not expected to reach here!");
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Test
    public void testEnabledRestButDisabledGroupClusterRead() {
        // REST should be disabled by default
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabledAndClusterReadDisabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        try {
            communicator.getInstanceInfo();
            fail("Rest is disabled. Not expected to reach here!");
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Test
    public void testGetInstanceName()
            throws IOException {
        final HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String expected = instance.getName();
        final String response = communicator.getInstanceInfo();
        final String nameFromResponse = Json.parse(response).asObject().get("name").asString();

        assertEquals(expected, nameFromResponse);
    }

    @Test
    public void testHeadRequestToInstance()
            throws IOException {
        final HTTPCommunicator communicator = new HTTPCommunicator(factory.newHecatonInstance(createConfigWithRestEnabled()));

        HTTPCommunicator.ConnectionResponse connectionResponse = communicator.headRequestToInstanceURI();
        assertEquals(200, connectionResponse.responseCode);
    }

}
