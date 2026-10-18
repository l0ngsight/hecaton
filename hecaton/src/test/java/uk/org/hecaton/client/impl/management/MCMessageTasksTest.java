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

import uk.org.hecaton.client.impl.ClientDelegatingFuture;
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
import uk.org.hecaton.client.impl.protocol.codec.MCDemoteDataMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetCPMembersCodec;
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
import uk.org.hecaton.client.impl.protocol.codec.MCPromoteToCPMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRemoveCPMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCResetCPSubsystemCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRunGcCodec;
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
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.instance.impl.HecatonInstanceProxy;
import uk.org.hecaton.internal.cluster.impl.VersionMismatchException;
import uk.org.hecaton.internal.management.dto.MCEventDTO;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.cp.CPSubsystemStubImpl.CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_CLIENTS;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmptyAfterTrim;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MCMessageTasksTest extends HecatonTestSupport {

    HecatonInstance client;
    HecatonInstance member;
    private TestHecatonFactory factory;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();

        member = factory.newHecatonInstance(smallInstanceConfig());
        client = factory.newHecatonClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testAddWanBatchPublisherConfigMessageTask() throws Exception {
        Random random = new Random();

        ClientMessage clientMessage = MCAddWanBatchPublisherConfigCodec.encodeRequest(
                randomString(),
                randomString(),
                randomString(),
                randomString(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                random.nextInt(),
                (byte) random.nextInt(2)
        );

        assertFailure(clientMessage, UnsupportedOperationException.class, "Adding new WAN config is not supported.");
    }

    @Test
    public void testApplyMCConfigMessageTask() throws Exception {
        ClientMessage clientMessage = MCApplyMCConfigCodec.encodeRequest(randomString(), 999, new ArrayList<>());
        assertFailure(clientMessage, IllegalArgumentException.class, "Unexpected client B/W list mode = [999]");
    }

    @Test
    public void testChangeClusterStateMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeClusterStateCodec.encodeRequest(888);
        assertFailure(clientMessage, IllegalArgumentException.class, "Unsupported ID value");
    }

    @Test
    public void testChangeClusterVersionMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeClusterVersionCodec.encodeRequest((byte) 8, (byte) 10);
        String expectedExceptionMsg = "Node's codebase version "
                + ((HecatonInstanceProxy) member).getOriginal().node.getVersion()
                + " is incompatible with the requested cluster version 8.10";
        assertFailure(clientMessage, VersionMismatchException.class, expectedExceptionMsg);
    }

    @Test
    public void testChangeWanReplicationStateMessageTask() throws Exception {
        ClientMessage clientMessage = MCChangeWanReplicationStateCodec.encodeRequest(
                randomString(),
                randomString(),
                (byte) 127
        );
        assertFailure(clientMessage, IllegalArgumentException.class, "Unexpected WAN publisher state = [127]");
    }

    @Test
    public void testCheckWanConsistencyMessageTask() throws Exception {
        ClientMessage clientMessage = MCCheckWanConsistencyCodec.encodeRequest(
                randomString(),
                randomString(),
                randomString()
        );
        assertFailure(clientMessage, UnsupportedOperationException.class, "Consistency check is not supported.");
    }

    @Test
    public void testClearWanQueuesMessageTask() throws Exception {
        ClientMessage clientMessage = MCClearWanQueuesCodec.encodeRequest(randomString(), randomString());
        assertFailure(clientMessage, UnsupportedOperationException.class, "Clearing WAN replication queues is not supported.");
    }

    @Test
    public void testGetClusterMetadataMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetClusterMetadataCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<MCGetClusterMetadataCodec.ResponseParameters> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetClusterMetadataCodec::decodeResponse
        );

        MCGetClusterMetadataCodec.ResponseParameters response = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertTrue(response.clusterTime > 0);
        assertEquals(0, response.currentState);
        assertEquals(BuildInfoProvider.getBuildInfo().getVersion(), response.memberVersion);
        assertNull(response.jetVersion);
    }

    @Test
    public void testGetCPMembersMessageTask() throws Exception {
        assertFailure(MCGetCPMembersCodec.encodeRequest(), UnsupportedOperationException.class, CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_CLIENTS);
    }

    @Test
    public void testGetMapConfigMessageTask() throws Exception {
        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(randomString());

        Config config = member.getConfig();
        String mapName = randomString();
        config.getMapConfig(mapName).setWanReplicationRef(wanRef);

        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetMapConfigCodec.encodeRequest(mapName),
                null
        );

        ClientDelegatingFuture<MCGetMapConfigCodec.ResponseParameters> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetMapConfigCodec::decodeResponse
        );

        MCGetMapConfigCodec.ResponseParameters response = future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertFalse(response.readBackupData);
        assertEquals(wanRef, response.wanReplicationRef);
    }

    @Test
    public void testGetMemberConfigMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetMemberConfigCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetMemberConfigCodec::decodeResponse
        );

        assertFalse(isNullOrEmptyAfterTrim(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS)));
    }

    @Test
    public void testGetSystemPropertiesMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetSystemPropertiesCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<List<Entry<String, String>>> future
                = new ClientDelegatingFuture<>(invocation.invoke(), getClientImpl().getSerializationService(),
                MCGetSystemPropertiesCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS).isEmpty());
    }

    @Test
    public void testGetThreadDumpMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetThreadDumpCodec.encodeRequest(false),
                null
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetThreadDumpCodec::decodeResponse
        );

        assertFalse(isNullOrEmptyAfterTrim(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS)));
    }

    @Test
    public void testGetTimedMemberStateMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCGetTimedMemberStateCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<String> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCGetTimedMemberStateCodec::decodeResponse
        );

        assertFalse(isNullOrEmptyAfterTrim(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS)));
    }

    @Test
    public void testHotRestartInterruptBackupMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCInterruptHotRestartBackupCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Void> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                clientMessage -> null
        );

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void testHotRestartTriggerBackupMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCTriggerHotRestartBackupCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Void> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                clientMessage -> null
        );

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void testHotRestartTriggerForceStartMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCTriggerForceStartCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Boolean> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCTriggerForceStartCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS));
    }

    @Test
    public void testHotRestartTriggerPartialStartMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCTriggerPartialStartCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Boolean> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCTriggerPartialStartCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS));
    }

    @Test
    public void testMatchMCConfigMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCMatchMCConfigCodec.encodeRequest(randomString()),
                null
        );

        ClientDelegatingFuture<Boolean> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCMatchMCConfigCodec::decodeResponse
        );

        assertFalse(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS));
    }

    @Test
    public void testPollMCEventsMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCPollMCEventsCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<List<MCEventDTO>> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                MCPollMCEventsCodec::decodeResponse
        );

        assertTrue(future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS).isEmpty());
    }

    @Test
    public void testPromoteLiteMemberMessageTask() throws Exception {
        assertFailure(MCPromoteLiteMemberCodec.encodeRequest(), IllegalStateException.class,
                member.getCluster().getLocalMember() + " is not a lite member!");
    }

    @Test
    public void testDemoteDataMemberMessageTask() throws Exception {
        Cluster cluster = member.getCluster();
        Address address = cluster.getLocalMember().getAddress();

        assertFailure(
                MCDemoteDataMemberCodec.encodeRequest(),
                IllegalStateException.class,
                "Cannot demote to lite member! Previous master was: " + address
                        + ", Current master is: " + address + ". Cluster state is " + cluster.getClusterState()
        );
    }

    @Test
    public void testPromoteToCPMemberMessageTask() throws Exception {
        assertFailure(MCPromoteToCPMemberCodec.encodeRequest(), UnsupportedOperationException.class, CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_CLIENTS);
    }

    @Test
    public void testRemoveCPMemberMessageTask() throws Exception {
        assertFailure(MCRemoveCPMemberCodec.encodeRequest(UUID.randomUUID()), UnsupportedOperationException.class, CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_CLIENTS);
    }

    @Test
    public void testResetCPSubsystemMessageTask() throws Exception {
        assertFailure(MCResetCPSubsystemCodec.encodeRequest(), UnsupportedOperationException.class, CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS_CLIENTS);
    }

    @Test
    public void testRunGCMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCRunGcCodec.encodeRequest(),
                null
        );

        ClientDelegatingFuture<Void> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                clientMessage -> null
        );

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void testShutdownClusterMessageTask() {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCShutdownClusterCodec.encodeRequest(),
                null
        );

        invocation.invoke();

        assertTrueEventually(() -> assertFalse(member.getLifecycleService().isRunning()));
    }

    @Test
    public void testShutdownMemberMessageTask() {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCShutdownMemberCodec.encodeRequest(),
                null
        );

        invocation.invoke();

        assertTrueEventually(() -> assertFalse(member.getLifecycleService().isRunning()));
    }

    @Test
    public void testUpdateMapConfigMessageTask() throws Exception {
        ClientInvocation invocation = new ClientInvocation(
                getClientImpl(),
                MCUpdateMapConfigCodec.encodeRequest(
                        randomString(),
                        100,
                        200,
                        0,
                        false,
                        100,
                        0,
                        null
                ),
                null
        );

        ClientDelegatingFuture<Void> future = new ClientDelegatingFuture<>(
                invocation.invoke(),
                getClientImpl().getSerializationService(),
                clientMessage -> null
        );

        future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void testWanSyncMapMessageTask() throws Exception {
        ClientMessage clientMessage = MCWanSyncMapCodec.encodeRequest(
                randomString(),
                randomString(),
                0,
                randomString()
        );
        assertFailure(clientMessage, UnsupportedOperationException.class, "WAN sync is not supported.");
    }

    private void assertFailure(ClientMessage clientMessage,
                               Class<? extends Exception> expectedExceptionType,
                               String expectedExceptionMsg) throws Exception {
        ClientInvocation invocation = new ClientInvocation(getClientImpl(), clientMessage, null);
        ClientInvocationFuture future = invocation.invoke();
        try {
            future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            fail("Execution was successful whereas failure was expected.");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue("Cause is of type " + cause.getClass(),
                    cause.getClass().isAssignableFrom(expectedExceptionType));
            assertContains(cause.getMessage(), expectedExceptionMsg);
        }
    }

    private HecatonClientInstanceImpl getClientImpl() {
        return ((HecatonClientProxy) client).client;
    }
}
