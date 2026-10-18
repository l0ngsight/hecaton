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

package uk.org.hecaton.client.txn;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.connection.ClientConnectionManager;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.util.AbstractLoadBalancer;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSingleMemberTxnTest extends ClientTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testUniSocketClient_shouldNotOpenANewConnection() {
        hecatonFactory.newHecatonInstance();
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        //try to force second member to connect when exist
        config.setLoadBalancer(new AbstractLoadBalancer() {
            @Override
            public Member next() {
                Member[] members = getMembers();
                if (members == null || members.length == 0) {
                    return null;
                }
                if (members.length == 2) {
                    return members[1];
                }
                return members[0];
            }

            @Override
            public Member nextDataMember() {
                Member[] members = getDataMembers();
                if (members == null || members.length == 0) {
                    return null;
                }
                if (members.length == 2) {
                    return members[1];
                }
                return members[0];
            }

            @Override
            public boolean canGetNextDataMember() {
                return true;
            }
        });
        final HecatonInstance client = hecatonFactory.newHecatonClient(config);
        hecatonFactory.newHecatonInstance();

        assertTrueEventually(() -> assertEquals(2, client.getCluster().getMembers().size()));


        ClientConnectionManager connectionManager = getHecatonClientInstanceImpl(client).getConnectionManager();

        TransactionContext context = client.newTransactionContext();

        context.beginTransaction();
        context.commitTransaction();

        assertEquals(1, connectionManager.getActiveConnections().size());
    }
}
