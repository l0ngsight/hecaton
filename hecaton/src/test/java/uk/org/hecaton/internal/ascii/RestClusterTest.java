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

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.RestApiConfig;
import uk.org.hecaton.config.RestServerEndpointConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.internal.ascii.HTTPCommunicator.ConnectionResponse;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonArray;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.spi.properties.ClusterProperty;
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
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static uk.org.hecaton.test.HecatonTestSupport.assertClusterStateEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertContains;
import static uk.org.hecaton.test.HecatonTestSupport.assertContainsAll;
import static uk.org.hecaton.test.HecatonTestSupport.assertFalseEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertOpenEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertThrows;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static uk.org.hecaton.test.HecatonTestSupport.spawn;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class RestClusterTest {

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
        return smallInstanceConfig();
    }

    protected Config createConfigWithRestEnabled() {
        Config config = createConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected String getPassword() {
        // Community version doesn't check the password.
        return "";
    }

    @Test
    public void testDisabledRest() {
        // REST should be disabled by default
        HecatonInstance instance = factory.newHecatonInstance(createConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        try {
            communicator.getClusterInfo();
            fail("Rest is disabled. Not expected to reach here!");
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Test
    public void testClusterInfo_whenAdvancedNetworkWithoutClientEndpoint() throws Exception {
        // when advanced network config is enabled and no client endpoint is defined
        // then client connections are reported as 0
        Config config = createConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
              .setRestEndpointConfig(new RestServerEndpointConfig()
                      .setPort(9999)
                      .enableAllGroups());
        HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        String response = communicator.getClusterInfo();
        JsonObject json = Json.parse(response).asObject();
        assertEquals(0, json.getInt("connectionCount", -1));
    }

    @Test
    public void testClusterShutdown() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HecatonInstance instance1 = factory.newHecatonInstance(config);
        final HecatonInstance instance2 = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance2);


        ConnectionResponse response = communicator.shutdownCluster(config.getClusterName(), getPassword());
        assertSuccessJson(response);
        assertTrueEventually(() -> {
            assertFalse(instance1.getLifecycleService().isRunning());
            assertFalse(instance2.getLifecycleService().isRunning());
        });
    }

    @Test
    public void testClusterShutdownFailure() throws Exception {
        Config config = createConfigWithRestEnabled();
        AtomicReference<HecatonInstance> member1 = new AtomicReference<>();
        AtomicReference<HecatonInstance> member2 = new AtomicReference<>();

        int port = config.getNetworkConfig().getPort() + 1;
        // for tests which use advanced network, change the port from the
        // member-member connection.
        if (config.getAdvancedNetworkConfig().isEnabled()) {
            config.getAdvancedNetworkConfig().getRestEndpointConfig().setPort(8080);
            port = 8080;
        }

        // stall the join process with these properties
        config.getProperties().setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "5");
        config.getProperties().setProperty(ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED.getName(), "false");

        // create two members async, separate spawns as the join wait will block
        spawn(() -> {
            member1.set(Hecaton.newHecatonInstance(config));
        });
        spawn(() -> {
            member2.set(Hecaton.newHecatonInstance(config));
        });

        HTTPCommunicator communicator = new HTTPCommunicator(port);
        // verify the connection is up - given nodes started async
        assertFalseEventually(() -> {
            assertThrows(Exception.class, communicator::getInstanceInfo);
        });

        HTTPCommunicator.ConnectionResponse response = communicator.shutdownCluster(config.getClusterName(), getPassword());
        assertFailureJson(response, "message", "Member has not yet joined cluster!");

        // make sure member creation is completed and running
        assertTrueEventually(() -> {
            assertNotNull("member1 was null", member1.get());
            assertNotNull("member2 was null", member2.get());
            assertTrue("member1 has been shutdown", member1.get().getLifecycleService().isRunning());
            assertTrue("member2 has been shutdown", member2.get().getLifecycleService().isRunning());
        });

        member1.get().shutdown();
        member2.get().shutdown();
    }


    @Test
    public void testGetClusterState() throws Exception {
        Config config = createConfigWithRestEnabled();
        HecatonInstance instance1 = factory.newHecatonInstance(config);
        HecatonInstance instance2 = factory.newHecatonInstance(config);
        String clusterName = config.getClusterName();
        HTTPCommunicator communicator1 = new HTTPCommunicator(instance1);
        HTTPCommunicator communicator2 = new HTTPCommunicator(instance2);

        instance1.getCluster().changeClusterState(ClusterState.FROZEN);
        ConnectionResponse resp1 = communicator1.getClusterState(clusterName, getPassword());
        assertSuccessJson(resp1, "state", "frozen");

        instance1.getCluster().changeClusterState(ClusterState.PASSIVE);
        ConnectionResponse resp2 = communicator2.getClusterState(clusterName, getPassword());
        assertSuccessJson(resp2, "state", "passive");
    }

    @Test
    public void testChangeClusterState() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HecatonInstance instance1 = factory.newHecatonInstance(config);
        final HecatonInstance instance2 = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);
        String clusterName = config.getClusterName();

        ConnectionResponse resp = communicator.changeClusterState(clusterName, getPassword(), "frozen");
        assertSuccessJson(resp, "state", "frozen");

        assertClusterStateEventually(ClusterState.FROZEN, instance1);
        assertClusterStateEventually(ClusterState.FROZEN, instance2);
    }

    @Test
    public void testGetClusterVersion() throws IOException {
        final HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertJsonContains(communicator.getClusterVersion(),
                "status", "success",
                "version", instance.getCluster().getClusterVersion().toString());
    }

    @Test
    public void testChangeClusterVersion() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HecatonInstance instance = factory.newHecatonInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String clusterName = config.getClusterName();
        ConnectionResponse resp = communicator.changeClusterVersion(clusterName, getPassword(),
                instance.getCluster().getClusterVersion().toString());
        assertSuccessJson(resp, "version", instance.getCluster().getClusterVersion().toString());
    }

    @Test
    public void testHotBackup() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HecatonInstance instance = factory.newHecatonInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String clusterName = config.getClusterName();
        ConnectionResponse resp = communicator.hotBackup(clusterName, getPassword());
        assertSuccessJson(resp);

        ConnectionResponse resp1 = communicator.hotBackupInterrupt(clusterName, getPassword());
        assertSuccessJson(resp1);
    }

    @Test
    public void testForceAndPartialStart() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HecatonInstance instance = factory.newHecatonInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String clusterName = config.getClusterName();
        ConnectionResponse resp1 = communicator.forceStart(clusterName, getPassword());
        assertEquals(HttpURLConnection.HTTP_OK, resp1.responseCode);
        assertJsonContains(resp1.response, "status", "fail");

        ConnectionResponse resp2 = communicator.partialStart(clusterName, getPassword());
        assertEquals(HttpURLConnection.HTTP_OK, resp2.responseCode);
        assertJsonContains(resp2.response, "status", "fail");
    }

    @Test
    public void testListNodes() throws Exception {
        Config config = createConfigWithRestEnabled();
        HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HecatonTestSupport.waitInstanceForSafeState(instance);
        String clusterName = config.getClusterName();
        ConnectionResponse resp = communicator.listClusterNodes(clusterName, getPassword());
        assertSuccessJson(resp,
                "response", String.format("[%s]\n%s\n%s", instance.getCluster().getLocalMember(),
                        BuildInfoProvider.getBuildInfo().getVersion(),
                        System.getProperty("java.version")));
    }

    @Test
    public void testShutdownNode() {
        Config config = createConfigWithRestEnabled();
        HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        instance.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                shutdownLatch.countDown();
            }
        });
        String clusterName = config.getClusterName();
        try {
            assertJsonContains(communicator.shutdownMember(clusterName, getPassword()).response, "status", "success");
        } catch (IOException ignored) {
            // exception is also a possible outcome when a node shut down before it has a chance
            // to send a response back to a client.
        }


        assertOpenEventually(shutdownLatch);
        assertFalse(instance.getLifecycleService().isRunning());
    }

    @Test
    public void simpleHealthCheck() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String result = communicator.getClusterHealth();

        JsonObject jsonResult = assertJsonContains(result,
                "nodeState", "ACTIVE",
                "clusterState", "ACTIVE");
        assertTrue(jsonResult.getBoolean("clusterSafe", false));
        assertEquals(0, jsonResult.getInt("migrationQueueSize", -1));
        assertEquals(1, jsonResult.getInt("clusterSize", -1));
    }

    @Test
    public void healthCheckWithPathParameters() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        assertEquals("\"ACTIVE\"", communicator.getClusterHealth("/node-state"));
        assertEquals("\"ACTIVE\"", communicator.getClusterHealth("/cluster-state"));
        assertEquals(HttpURLConnection.HTTP_OK, communicator.getClusterHealthResponseCode("/cluster-safe"));
        assertEquals("0", communicator.getClusterHealth("/migration-queue-size"));
        assertEquals("1", communicator.getClusterHealth("/cluster-size"));
    }

    @Test
    public void healthCheckWithUnknownPathParameter() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, communicator.getClusterHealthResponseCode("/unknown-parameter"));
    }

    @Test(expected = IOException.class)
    public void fail_with_deactivatedHealthCheck() throws Exception {
        // Healthcheck REST URL is deactivated by default - no passed config on purpose
        HecatonInstance instance = factory.newHecatonInstance(null);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        communicator.getClusterHealth();
    }

    @Test
    public void fail_on_healthcheck_url_with_garbage() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, communicator.getFailingClusterHealthWithTrailingGarbage());
    }

    @Test
    public void testHeadRequest_ClusterVersion() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.headRequestToClusterVersionURI().responseCode);
    }

    @Test
    public void testHeadRequest_ClusterInfo() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.headRequestToClusterInfoURI().responseCode);
    }

    @Test
    public void testHeadRequest_ClusterHealth() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse response = communicator.headRequestToClusterHealthURI();
        assertEquals(HttpURLConnection.HTTP_OK, response.responseCode);
        assertEquals(1, response.responseHeaders.get("Hecaton-NodeState").size());
        assertContains(response.responseHeaders.get("Hecaton-NodeState"), "ACTIVE");
        assertEquals(1, response.responseHeaders.get("Hecaton-ClusterState").size());
        assertContains(response.responseHeaders.get("Hecaton-ClusterState"), "ACTIVE");
        assertEquals(1, response.responseHeaders.get("Hecaton-ClusterSize").size());
        assertContains(response.responseHeaders.get("Hecaton-ClusterSize"), "2");
        assertEquals(1, response.responseHeaders.get("Hecaton-MigrationQueueSize").size());
        assertContains(response.responseHeaders.get("Hecaton-MigrationQueueSize"), "0");
    }

    @Test
    public void testHeadRequest_GarbageClusterHealth() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HTTP_NOT_FOUND, communicator.headRequestToGarbageClusterHealthURI().responseCode);
    }

    @Test
    public void http_get_returns_response_code_200_when_member_is_ready_to_use() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        int healthReadyResponseCode = communicator.getHealthReadyResponseCode();
        assertEquals(HttpURLConnection.HTTP_OK, healthReadyResponseCode);
    }

    @Test
    public void testSetLicenseKey() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse response =
                communicator.setLicense(config.getClusterName(), getPassword(), "whatever");
        assertSuccessJson(response);
    }

    @Test
    public void testConfigReload() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse response = communicator.configReload(config.getClusterName(), getPassword());

        // Reload is enterprise feature. Should fail here.
        assertJsonContains(response.response, "status", "fail",
                "message", HecatonFeature.CONFIGURATION_RELOAD.notAvailableMessage());
    }

    @Test
    public void testConfigUpdate() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse response = communicator.configUpdate(
                config.getClusterName(), getPassword(), "hecaton:\n"
        );

        // Reload is enterprise feature. Should fail here.
        assertJsonContains(response.response, "status", "fail",
                "message", HecatonFeature.CONFIGURATION_RELOAD.notAvailableMessage());
    }

    @Test
    public void testGetTcpIpMemberList() throws Exception {
        Config config = createConfigWithRestEnabled();
        List<String> members = Arrays.asList("localhost:5701", "localhost:5702", "localhost:5703", "localhost:5704");
        if (config.getAdvancedNetworkConfig().isEnabled()) {
            config.getAdvancedNetworkConfig().getJoin().getTcpIpConfig()
                    .setMembers(members);
        } else {
            config.getNetworkConfig().getJoin().getTcpIpConfig()
                    .setMembers(members);
        }

        HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse resp = communicator.getTcpIpMemberList();
        assertSuccessJson(resp);
        assertContainsAll(((JsonArray) Json.parse(resp.response).asObject().get("member-list"))
                        .values().stream().map(JsonValue::asString).collect(Collectors.toList()),
                members);
    }

    @Test
    public void testUpdateTcpIpMemberList() throws Exception {
        Config config = createConfigWithRestEnabled();
        HecatonInstance instance = factory.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        List<String> expectedMembers = Arrays.asList("localhost:8001", "localhost:9001", "localhost:10001");
        ConnectionResponse resp = communicator.updateTcpIpMemberList(
                config.getClusterName(),
                getPassword(),
                "localhost:8001, localhost:9001, localhost:10001");

        assertSuccessJson(resp);
        assertContainsAll(((JsonArray) Json.parse(resp.response).asObject().get("member-list"))
                        .values().stream().map(JsonValue::asString).collect(Collectors.toList()),
                expectedMembers);
        if (config.getAdvancedNetworkConfig().isEnabled()) {
            assertContainsAll(instance.getConfig().getAdvancedNetworkConfig().getJoin().getTcpIpConfig().getMembers(),
                    expectedMembers);
        } else {
            assertContainsAll(instance.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().getMembers(),
                    expectedMembers);
        }
    }

    @Test
    public void testGetTcpIpMemberListWhenTcpIpJoinIsNotUsed() throws Exception {
        Config config = createConfigWithRestEnabled();
        // disable all join mechanisms
        config.setProperty("hecaton.wait.seconds.before.join", "0");
        config.getNetworkConfig().setPort(10111);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getKubernetesConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HecatonInstance instance = null;
        try {
            instance = Hecaton.newHecatonInstance(config);
            HTTPCommunicator communicator = new HTTPCommunicator(instance);
            ConnectionResponse resp = communicator.getTcpIpMemberList();
            assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, resp.responseCode);
            assertJsonContains(resp.response, "message",
                    "TCP-IP join mechanism is not enabled in the cluster.");
        } finally {
            if (instance != null) {
                instance.getLifecycleService().terminate();
            }
        }
    }

    @Test
    public void testUpdateTcpIpMemberListWhenTcpIpJoinIsNotUsed() throws Exception {
        Config config = createConfigWithRestEnabled();
        // disable all join mechanisms
        config.setProperty("hecaton.wait.seconds.before.join", "0");
        config.getNetworkConfig().setPort(10112);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getKubernetesConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HecatonInstance instance = null;
        try {
            instance = Hecaton.newHecatonInstance(config);
            HTTPCommunicator communicator = new HTTPCommunicator(instance);
            ConnectionResponse resp = communicator.updateTcpIpMemberList(
                    config.getClusterName(),
                    getPassword(),
                    "localhost:8001, localhost:9001, localhost:10001");
            assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, resp.responseCode);
            assertJsonContains(resp.response, "status", "fail", "message",
                    "TCP-IP join mechanism is not enabled in the cluster.");
        } finally {
            if (instance != null) {
                instance.getLifecycleService().terminate();
            }
        }

    }

    private JsonObject assertJsonContains(String json, String... attributesAndValues) {
        JsonObject object = Json.parse(json).asObject();
        for (int i = 0; i < attributesAndValues.length; ) {
            String key = attributesAndValues[i++];
            String expectedValue = attributesAndValues[i++];
            assertEquals(expectedValue, object.getString(key, null));
        }
        return object;
    }

    private void assertSuccessJson(ConnectionResponse resp, String... attributesAndValues) {
        assertEquals(HttpURLConnection.HTTP_OK, resp.responseCode);
        assertJsonContains(resp.response, "status", "success");
        if (attributesAndValues.length > 0) {
            assertJsonContains(resp.response, attributesAndValues);
        }
    }

    void assertFailureJson(ConnectionResponse resp, String... attributesAndValues) {
        assertEquals(HttpURLConnection.HTTP_CONFLICT, resp.responseCode);
        assertJsonContains(resp.response, "status", "fail");
        if (attributesAndValues.length > 0) {
            assertJsonContains(resp.response, attributesAndValues);
        }
    }
}
