/*
 * Copyright (c) 2026, The Hecaton Authors.
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
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestAwareInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static uk.org.hecaton.instance.EndpointQualifier.REST;
import static uk.org.hecaton.test.Accessors.getNode;

/**
 * Requests to the Hazelcast REST paths ({@code /hazelcast/...}) are served as the Hecaton
 * ones ({@code /hecaton/...}), including the endpoint-group checks.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class RestPathAliasTest {

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeClass
    public static void beforeClass() {
        Hecaton.shutdownAll();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void healthCheck_onLegacyPath() throws Exception {
        String base = baseUrl(factory.newHecatonInstance(config(RestEndpointGroup.HEALTH_CHECK)));

        HttpResponse<String> legacy = get(base + "/hazelcast/health/ready");
        HttpResponse<String> current = get(base + "/hecaton/health/ready");

        assertEquals(200, legacy.statusCode());
        assertEquals(current.statusCode(), legacy.statusCode());
    }

    @Test
    public void dataEndpoints_onLegacyPath() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(config(RestEndpointGroup.DATA));
        instance.getMap("m").put("k", "v");

        HttpResponse<String> response = get(baseUrl(instance) + "/hazelcast/rest/maps/m/k");

        assertEquals(200, response.statusCode());
        assertEquals("v", response.body());
    }

    @Test
    public void legacyPath_respectsEndpointGroups() {
        // DATA enabled, HEALTH_CHECK disabled: the alias must not open the health endpoints.
        Config config = config(RestEndpointGroup.DATA);
        config.getNetworkConfig().getRestApiConfig().disableGroups(RestEndpointGroup.HEALTH_CHECK);
        String base = baseUrl(factory.newHecatonInstance(config));

        // The member closes the connection without a response.
        assertThrows(IOException.class, () -> get(base + "/hazelcast/health/ready"));
    }

    private static Config config(RestEndpointGroup group) {
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(new RestApiConfig().setEnabled(true).enableGroups(group));
        return config;
    }

    private static String baseUrl(HecatonInstance instance) {
        return "http:/" + getNode(instance).getClusterService().getLocalMember().getSocketAddress(REST);
    }

    private HttpResponse<String> get(String url) throws IOException, InterruptedException {
        return client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
