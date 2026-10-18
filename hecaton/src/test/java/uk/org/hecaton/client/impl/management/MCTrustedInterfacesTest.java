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

package uk.org.hecaton.client.impl.management;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.ClientDelegatingFuture;
import uk.org.hecaton.client.impl.ClientEndpointImpl;
import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCApplyMCConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCChangeClusterStateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCChangeClusterVersionCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCChangeWanReplicationStateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCCheckWanConsistencyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCClearWanQueuesCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetClusterMetadataCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetMemberConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetSystemPropertiesCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetThreadDumpCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetTimedMemberStateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCInterruptHotRestartBackupCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCMatchMCConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCPollMCEventsCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCPromoteLiteMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCReadMetricsCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRunGcCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRunScriptCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCShutdownClusterCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCShutdownMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCTriggerForceStartCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCTriggerHotRestartBackupCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCTriggerPartialStartCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCUpdateMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCWanSyncMapCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nio.ConnectionType;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class MCTrustedInterfacesTest extends HecatonTestSupport {

    HecatonInstance client;
    HecatonInstance member;
    private TestHecatonFactory factory;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();

        Config config = smallInstanceConfig();
        config.getManagementCenterConfig().addTrustedInterface("222.222.222.222");
        member = factory.newHecatonInstance(config);
        client = factory.newHecatonClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testGetSystemPropertiesMessageTask_passing() throws Exception {
        HecatonInstance client = factory.newHecatonClient(new ClientConfig(), "222.222.222.222");
        HecatonClientInstanceImpl clientImpl = ((HecatonClientProxy) client).client;
        ClientInvocation invocation = new ClientInvocation(
                clientImpl,
                MCGetSystemPropertiesCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<List<Map.Entry<String, String>>> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                clientImpl.getSerializationService(),
                MCGetSystemPropertiesCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS).isEmpty());
    }


    @Test
    public void testAddWanBatchPublisherConfigMessageTask() throws Exception {
        Random random = new Random();

        ClientMessage clientMessage = MCAddWanBatchPublisherConfigCodec.encodeRequest(randomString(), randomString(),
                randomString(), randomString(), random.nextInt(), random.nextInt(), random.nextInt(), random.nextInt(),
                random.nextInt(), random.nextInt(), (byte) random.nextInt(2));

        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testApplyMCConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCApplyMCConfigCodec.encodeRequest(randomString(), 999, new ArrayList<>()));
    }

    @Test
    public void testChangeClusterStateMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeClusterStateCodec.encodeRequest(888);
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testChangeClusterVersionMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeClusterVersionCodec.encodeRequest((byte) 8, (byte) 10);
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testChangeWanReplicationStateMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeWanReplicationStateCodec.encodeRequest(randomString(), randomString(),
                (byte) 127);
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testCheckWanConsistencyMessageTask() throws Exception {
        ClientMessage clientMessage = MCCheckWanConsistencyCodec.encodeRequest(randomString(), randomString(), randomString());
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testClearWanQueuesMessageTask() throws Exception {
        ClientMessage clientMessage = MCClearWanQueuesCodec.encodeRequest(randomString(), randomString());
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testGetClusterMetadataMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetClusterMetadataCodec.encodeRequest());
    }

    @Test
    public void testGetMapConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetMapConfigCodec.encodeRequest(randomString()));
    }

    @Test
    public void testGetMemberConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetMemberConfigCodec.encodeRequest());
    }

    @Test
    public void testGetSystemPropertiesMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetSystemPropertiesCodec.encodeRequest());
    }

    @Test
    public void testGetThreadDumpMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetThreadDumpCodec.encodeRequest(false));
    }

    @Test
    public void testGetTimedMemberStateMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCGetTimedMemberStateCodec.encodeRequest());
    }

    @Test
    public void testHotRestartInterruptBackupMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCInterruptHotRestartBackupCodec.encodeRequest());
    }

    @Test
    public void testHotRestartTriggerBackupMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCTriggerHotRestartBackupCodec.encodeRequest());
    }

    @Test
    public void testHotRestartTriggerForceStartMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCTriggerForceStartCodec.encodeRequest());
    }

    @Test
    public void testHotRestartTriggerPartialStartMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCTriggerPartialStartCodec.encodeRequest());
    }

    @Test
    public void testMatchMCConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCMatchMCConfigCodec.encodeRequest(randomString()));
    }

    @Test
    public void testPollMCEventsMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCPollMCEventsCodec.encodeRequest());
    }

    @Test
    public void testPromoteLiteMemberMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCPromoteLiteMemberCodec.encodeRequest());
    }

    @Test
    public void testRunConsoleCommandMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCRunConsoleCommandCodec.encodeRequest(randomString(), "help"));
    }

    @Test
    public void testRunGCMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCRunGcCodec.encodeRequest());
    }

    @Test
    public void testRunScriptMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCRunScriptCodec.encodeRequest(randomString(), randomString()));
    }

    @Test
    public void testShutdownClusterMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCShutdownClusterCodec.encodeRequest());
        assertTrue(member.getLifecycleService().isRunning());
    }

    @Test
    public void testShutdownMemberMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCShutdownMemberCodec.encodeRequest());
        assertTrue(member.getLifecycleService().isRunning());
    }

    @Test
    public void testUpdateMapConfigMessageTask() throws Exception {
        assertFailureOnUntrustedInterface(MCUpdateMapConfigCodec.encodeRequest(randomString(), 100, 200, 0, false, 100, 0, null));
    }

    @Test
    public void testWanSyncMapMessageTask() throws Exception {
        ClientMessage clientMessage = MCWanSyncMapCodec.encodeRequest(randomString(), randomString(), 0, randomString());
        assertFailureOnUntrustedInterface(clientMessage);
    }

    @Test
    public void testReadMetrics() throws Exception {
        assertFailureOnUntrustedInterface(MCReadMetricsCodec.encodeRequest(randomUUID(), 0L));
    }

    @Test
    public void testBind_whenMCCLCAddressIsNotTrusted_returnFalse() {
        ClientEngine clientEngine = getNode(member).getClientEngine();
        ServerConnection mockConnection = mock(ServerConnection.class);
        when(mockConnection.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 1234));
        when(mockConnection.getConnectionType()).thenReturn(ConnectionType.MC_CL_CLIENT);

        ClientEndpointImpl mcClc = new ClientEndpointImpl(clientEngine, getNodeEngineImpl(member), mockConnection);
        mcClc.authenticated(randomUUID(), null, null, 0L, null, null, null, false);

        assertFalse(clientEngine.bind(mcClc));
    }

    @Test
    public void testBind_whenMCCLCAddressIsTrusted_returnTrue() {
        ClientEngine clientEngine = getNode(member).getClientEngine();
        ServerConnection mockConnection = mock(ServerConnection.class);
        when(mockConnection.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("222.222.222.222", 1234));
        when(mockConnection.getConnectionType()).thenReturn(ConnectionType.MC_CL_CLIENT);

        ClientEndpointImpl mcClc = new ClientEndpointImpl(clientEngine, getNodeEngineImpl(member), mockConnection);
        mcClc.authenticated(randomUUID(), null, null, 0L, null, null, null, false);

        assertTrue(clientEngine.bind(mcClc));
    }

    private void assertFailureOnUntrustedInterface(ClientMessage clientMessage) throws Exception {
        ClientInvocation invocation = new ClientInvocation(((HecatonClientProxy) client).client, clientMessage, null);
        ClientInvocationFuture future = invocation.invoke();
        try {
            future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            fail("AccessControlException was expected.");
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(AccessControlException.class);
        }
    }
}
