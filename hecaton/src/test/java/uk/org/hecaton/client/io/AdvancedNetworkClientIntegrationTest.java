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

package uk.org.hecaton.client.io;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MembershipEvent;
import uk.org.hecaton.cluster.MembershipListener;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.partition.Partition;
import uk.org.hecaton.scheduledexecutor.IScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.IScheduledFuture;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.instance.EndpointQualifier.CLIENT;
import static uk.org.hecaton.test.HecatonTestSupport.assertClusterSizeEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertContains;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.randomMapName;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class AdvancedNetworkClientIntegrationTest {

    private static final int CLUSTER_SIZE = 3;
    private static final int BASE_CLIENT_PORT = 9090;

    private final HecatonInstance[] instances = new HecatonInstance[CLUSTER_SIZE];
    private HecatonInstance client;

    @Before
    public void setup() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i] = Hecaton.newHecatonInstance(getConfig());
        }
        assertClusterSizeEventually(CLUSTER_SIZE, instances);
    }

    @After
    public void tearDown() {
        client.shutdown();
        instances[0].getCluster().shutdown();
    }

    @Test
    @Category(QuickTest.class)
    public void clientSmokeTest() {
        client = HecatonClient.newHecatonClient(getClientConfig());
        IMap<Integer, Integer> map = client.getMap("test");
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        assertEquals(1000, map.size());
    }

    @Test
    public void testClientMembersList() {
        client = HecatonClient.newHecatonClient(getClientConfig());
        Set<Member> clientViewOfMembers = client.getCluster().getMembers();
        Set<Member> members = instances[1].getCluster().getMembers();

        Set<Address> clientViewOfAddresses = new HashSet<>();
        for (Member member : clientViewOfMembers) {
            clientViewOfAddresses.add(member.getAddress());
        }

        for (Member member : members) {
            assertContains(clientViewOfAddresses, member.getAddressMap().get(CLIENT));
        }
    }

    @Test
    public void testClientViewOfAddressMap() {
        client = HecatonClient.newHecatonClient(getClientConfig());
        for (Member member: client.getCluster().getMembers()) {
            assertEquals(member.getAddress(), member.getAddressMap().get(CLIENT));
            int memberPort = member.getAddressMap().get(EndpointQualifier.MEMBER).getPort();
            assertTrue("member address port is between 5700 and 6000", 5700  <= memberPort && memberPort <= 6000);
        }
    }

    @Test
    public void testClientViewOfAddressMapWhenMemberWithNoClientEndpointExist() {
        HecatonInstance memberWithNoClientEndpoint = Hecaton.newHecatonInstance(getMemberConfigWithNoClientEndpoint());
        client = HecatonClient.newHecatonClient(getClientConfig());
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(3, members.size());
        Member noClientEndpointMember = memberWithNoClientEndpoint.getCluster().getLocalMember();
        assertFalse(members.contains(noClientEndpointMember));
        for (Member member : members) {
            assertEquals(member.getAddress(), member.getAddressMap().get(CLIENT));
            int memberPort = member.getAddressMap().get(EndpointQualifier.MEMBER).getPort();
            assertTrue("member address port is between 5700 and 6000", 5700 <= memberPort && memberPort <= 6000);
        }

        // try to insert some data to the member which does not have client endpoint
        int key = findNextKeyForMember(memberWithNoClientEndpoint, noClientEndpointMember);
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(key, key);
        assertEquals(key, map.get(key).intValue());
    }

    @Test
    public void testClientMembershipEvent() {
        client = HecatonClient.newHecatonClient(getClientConfig());
        AtomicReference<Member> memberAdded = new AtomicReference<>();
        AtomicReference<Member> memberRemoved = new AtomicReference<>();
        Address removedMemberAddress = instances[2].getCluster().getLocalMember().getAddressMap().get(CLIENT);

        client.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                memberAdded.set(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemoved.set(membershipEvent.getMember());
            }

        });

        instances[2].shutdown();
        assertClusterSizeEventually(2, instances[0]);
        assertTrueEventually(() -> assertNotNull(memberRemoved.get()));

        assertEquals(memberRemoved.get().getAddress(), removedMemberAddress);

        instances[2] = Hecaton.newHecatonInstance(getConfig());
        assertClusterSizeEventually(3, instances);

        assertTrueEventually(() -> assertNotNull(memberAdded.get()));
        assertEquals(memberAdded.get().getAddress(), instances[2].getCluster().getLocalMember().getAddressMap().get(CLIENT));
    }

    @Test
    public void testPartitions() {
        client = HecatonClient.newHecatonClient(getClientConfig());
        Iterator<Partition> memberPartitions = instances[0].getPartitionService().getPartitions().iterator();
        Set<Partition> partitions = client.getPartitionService().getPartitions();

        for (Partition partition : partitions) {
            Partition memberPartition = memberPartitions.next();
            assertEquals(memberPartition.getPartitionId(), partition.getPartitionId());
            assertTrueEventually(() -> {
                Member owner = partition.getOwner();
                assertNotNull(owner);
                assertEquals(memberPartition.getOwner().getAddressMap().get(CLIENT), owner.getAddress());
            });
        }
    }

    @Test
    public void testGetScheduledFutures() throws Exception {
        client = HecatonClient.newHecatonClient(getClientConfig());
        IScheduledExecutorService executorService = client.getScheduledExecutorService("test");
        Member targetMember = client.getCluster().getMembers().iterator().next();

        IScheduledFuture<Address> future = executorService.scheduleOnMember(new ReportExecutionMember(),
                targetMember, 3, TimeUnit.SECONDS);
        Map<Member, List<IScheduledFuture<Address>>> futures = executorService.getAllScheduledFutures();

        assertContains(futures.keySet(), targetMember);

        IScheduledFuture<Address> futureOfMember = futures.get(targetMember).get(0);
        assertEquals(futureOfMember.get(), future.get());
    }

    @Test
    public void testScheduleOnMember() throws Exception {
        client = HecatonClient.newHecatonClient(getClientConfig());
        IScheduledExecutorService executorService = client.getScheduledExecutorService("test");
        Member targetMember = client.getCluster().getMembers().iterator().next();

        IScheduledFuture<Address> future = executorService.scheduleOnMember(new ReportExecutionMember(),
                targetMember, 3, TimeUnit.SECONDS);

        assertEquals(targetMember.getUuid(), future.getHandler().getUuid());

        Address clusterMemberAddress = null;
        for (HecatonInstance instance : instances) {
            if (targetMember.getAddress().equals(instance.getCluster().getLocalMember().getAddressMap().get(CLIENT))) {
                clusterMemberAddress = instance.getCluster().getLocalMember().getAddress();
            }
        }

        assertEquals(future.get(), clusterMemberAddress);

    }

    public static class ReportExecutionMember implements Callable<Address>, Serializable, HecatonInstanceAware {

        private volatile HecatonInstance instance;

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.instance = hecatonInstance;
        }

        @Override
        public Address call() {
            return instance.getCluster().getLocalMember().getAddress();
        }
    }

    private Config getConfig() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
              .setClientEndpointConfig(new ServerSocketEndpointConfig().setPort(BASE_CLIENT_PORT));
        config.getAdvancedNetworkConfig()
              .getJoin().getMulticastConfig().setEnabled(false);
        config.getAdvancedNetworkConfig()
              .getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        return config;
    }

    private Config getMemberConfigWithNoClientEndpoint() {
        Config config = smallInstanceConfig();
        //config.setLiteMember(true);
        config.getAdvancedNetworkConfig().setEnabled(true);
        config.getAdvancedNetworkConfig()
                .getJoin().getMulticastConfig().setEnabled(false);
        config.getAdvancedNetworkConfig()
                .getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        return config;
    }

    private ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:9090");
        return clientConfig;
    }

    protected int findNextKeyForMember(HecatonInstance instance, Member localMember) {
        int key = 0;
        while (!localMember.equals(instance.getPartitionService().getPartition(key).getOwner())) {
            key++;
        }
        return key;
    }
}
