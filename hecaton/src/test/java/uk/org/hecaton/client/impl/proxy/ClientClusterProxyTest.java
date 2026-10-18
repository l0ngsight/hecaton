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

package uk.org.hecaton.client.impl.proxy;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cluster.MembershipAdapter;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionOptions;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterProxyTest extends HecatonTestSupport {

    private TestHecatonFactory factory;

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    private HecatonInstance client() {
        factory = new TestHecatonFactory();
        Config config = new Config();
        String clusterAName = "HZ:CLUSTER";
        config.setClusterName(clusterAName);
        factory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(config.getClusterName());
        return factory.newHecatonClient(clientConfig);
    }

    @Test
    public void addMembershipListener() {
        UUID regId = client().getCluster().addMembershipListener(new MembershipAdapter());
        assertNotNull(regId);
    }

    @Test
    public void removeMembershipListener() {
        Cluster cluster = client().getCluster();
        UUID regId = cluster.addMembershipListener(new MembershipAdapter());
        assertTrue(cluster.removeMembershipListener(regId));
    }

    @Test
    public void getMembers() {
        assertEquals(1, client().getCluster().getMembers().size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getLocalMember() {
        client().getCluster().getLocalMember();
    }

    @Test
    public void getClusterTime() {
        assertTrue(client().getCluster().getClusterTime() > 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getClusterState() {
        client().getCluster().getClusterState();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void changeClusterState() {
        client().getCluster().changeClusterState(ClusterState.FROZEN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getClusterVersion() {
        client().getCluster().getClusterVersion();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void changeClusterStateWithOptions() {
        client().getCluster().changeClusterState(ClusterState.FROZEN, new TransactionOptions());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shutdown() {
        client().getCluster().shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shutdownWithOptions() {
        client().getCluster().shutdown(new TransactionOptions());
    }
}
