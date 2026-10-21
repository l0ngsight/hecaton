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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.RestApiConfig;
import uk.org.hecaton.config.RestEndpointGroup;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.ascii.HTTPCommunicator;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.management.events.Event;
import uk.org.hecaton.internal.management.events.WanConsistencyCheckIgnoredEvent;
import uk.org.hecaton.internal.management.events.WanSyncIgnoredEvent;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.internal.management.events.EventMetadata.EventType.WAN_CONSISTENCY_CHECK_IGNORED;
import static uk.org.hecaton.internal.management.events.EventMetadata.EventType.WAN_SYNC_IGNORED;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.TestEnvironment.HECATON_TEST_USE_NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
public class WanOpenSourceAntiEntropyMcEventsTest extends HecatonTestSupport {
    private static final String MAP_NAME = "map";
    private static final String WAN_REPLICATION_NAME = "wanRepName";
    private static final String WAN_PUBLISHER_ID = "wanPubId";

    private TestHecatonInstanceFactory factory;

    @After
    public void tearDown() {
        factory.shutdownAll();
        System.clearProperty(HECATON_TEST_USE_NETWORK);
    }

    @Test
    public void testConsistencyCheckAPI() {
        HecatonInstance hz = createHecatonInstance();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        WanReplicationService wanService = nodeEngine.getWanReplicationService();
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        // the map keeps no Merkle tree: the check is ignored
        assertNull(wanService.consistencyCheck(WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, MAP_NAME));
        assertOpenEventually(latch);

        Event event = events.get(0);
        assertTrue(event instanceof WanConsistencyCheckIgnoredEvent);
        WanConsistencyCheckIgnoredEvent checkIgnoredEvent = (WanConsistencyCheckIgnoredEvent) event;
        assertNotNull(checkIgnoredEvent.getUuid());
        assertEquals(MAP_NAME, checkIgnoredEvent.getMapName());
    }

    @Test
    public void testConsistencyCheckREST() throws Exception {
        System.setProperty(HECATON_TEST_USE_NETWORK, "true");

        HecatonInstance hz = createHecatonInstance(getConfigWithRest());
        HTTPCommunicator communicator = new HTTPCommunicator(hz);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        String jsonResult = communicator.wanMapConsistencyCheck(hz.getConfig().getClusterName(), "", WAN_REPLICATION_NAME,
                WAN_PUBLISHER_ID, MAP_NAME);
        assertOpenEventually(latch);

        JsonObject result = Json.parse(jsonResult).asObject();
        Event event = events.get(0);
        assertTrue(event instanceof WanConsistencyCheckIgnoredEvent);
        WanConsistencyCheckIgnoredEvent checkIgnoredEvent = (WanConsistencyCheckIgnoredEvent) event;
        assertNotNull(checkIgnoredEvent.getUuid());
        assertNull(result.getString("uuid", null));
        assertEquals(MAP_NAME, checkIgnoredEvent.getMapName());
        assertEquals(WAN_CONSISTENCY_CHECK_IGNORED, checkIgnoredEvent.getType());
    }

    @Test
    public void testSyncAPI() {
        HecatonInstance hz = createHecatonInstance();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        WanReplicationService wanService = nodeEngine.getWanReplicationService();
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        // there is no such WAN replication scheme
        assertThrows(InvalidConfigurationException.class,
                () -> wanService.syncMap(WAN_REPLICATION_NAME, WAN_PUBLISHER_ID, MAP_NAME));
        assertOpenEventually(latch);

        Event event = events.get(0);
        assertTrue(event instanceof WanSyncIgnoredEvent);
        WanSyncIgnoredEvent syncIgnoredEvent = (WanSyncIgnoredEvent) event;
        assertNotNull(syncIgnoredEvent.getUuid());
        assertEquals(MAP_NAME, syncIgnoredEvent.getMapName());
    }

    @Test
    public void testSyncREST() throws Exception {
        System.setProperty(HECATON_TEST_USE_NETWORK, "true");

        HecatonInstance hz = createHecatonInstance(getConfigWithRest());
        HTTPCommunicator communicator = new HTTPCommunicator(hz);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        String jsonResult = communicator.syncMapOverWAN(hz.getConfig().getClusterName(), "", WAN_REPLICATION_NAME,
                WAN_PUBLISHER_ID, MAP_NAME);
        assertOpenEventually(latch);

        JsonObject result = Json.parse(jsonResult).asObject();
        Event event = events.get(0);
        assertTrue(event instanceof WanSyncIgnoredEvent);
        WanSyncIgnoredEvent syncIgnoredEvent = (WanSyncIgnoredEvent) event;
        assertNotNull(syncIgnoredEvent.getUuid());
        assertNull(result.getString("uuid", null));
        assertEquals(MAP_NAME, syncIgnoredEvent.getMapName());
        assertEquals(WAN_SYNC_IGNORED, syncIgnoredEvent.getType());
    }

    @Test
    public void testAllMapsSyncAPI() {
        HecatonInstance hz = createHecatonInstance();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        WanReplicationService wanService = nodeEngine.getWanReplicationService();
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        assertThrows(InvalidConfigurationException.class,
                () -> wanService.syncAllMaps(WAN_REPLICATION_NAME, WAN_PUBLISHER_ID));
        assertOpenEventually(latch);

        Event event = events.get(0);
        assertTrue(event instanceof WanSyncIgnoredEvent);
        WanSyncIgnoredEvent syncIgnoredEvent = (WanSyncIgnoredEvent) event;
        assertNotNull(syncIgnoredEvent.getUuid());
        assertNull(syncIgnoredEvent.getMapName());
    }

    @Test
    public void testSyncAllREST() throws Exception {
        System.setProperty(HECATON_TEST_USE_NETWORK, "true");

        HecatonInstance hz = createHecatonInstance(getConfigWithRest());
        HTTPCommunicator communicator = new HTTPCommunicator(hz);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        ManagementCenterService mcService = nodeEngine.getManagementCenterService();

        List<Event> events = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(1);
        mcService.setEventListener(event -> {
            events.add(event);
            latch.countDown();
        });

        String jsonResult = communicator.syncMapsOverWAN(hz.getConfig().getClusterName(), "", WAN_REPLICATION_NAME,
                WAN_PUBLISHER_ID);
        assertOpenEventually(latch);

        JsonObject result = Json.parse(jsonResult).asObject();
        Event event = events.get(0);
        assertTrue(event instanceof WanSyncIgnoredEvent);
        WanSyncIgnoredEvent syncIgnoredEvent = (WanSyncIgnoredEvent) event;
        assertNotNull(syncIgnoredEvent.getUuid());
        assertNull(result.getString("uuid", null));
        assertNull(syncIgnoredEvent.getMapName());
        assertEquals(WAN_SYNC_IGNORED, syncIgnoredEvent.getType());
    }

    @Override
    protected HecatonInstance createHecatonInstance() {
        factory = new TestHecatonInstanceFactory();
        return factory.newHecatonInstance();
    }

    @Override
    protected HecatonInstance createHecatonInstance(Config config) {
        factory = new TestHecatonInstanceFactory();
        return factory.newHecatonInstance(config);
    }

    private Config getConfigWithRest() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getNetworkConfig().getJoin().getTcpIpConfig().clear();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableGroups(RestEndpointGroup.WAN);
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig()
                .setEnabled(false);
        joinConfig.getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1");
        return config;
    }

}
