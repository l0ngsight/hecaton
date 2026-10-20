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
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.util.AbstractLoadBalancer;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.transaction.TransactionalQueue;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MembershipAdapter;
import uk.org.hecaton.cluster.MembershipEvent;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.TransactionOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private HecatonInstance client;
    private HecatonInstance server;

    @Before
    public void setup() {
        server = hecatonFactory.newHecatonInstance();
        final ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setRedoOperation(true);
        //always start the txn on first member
        config.setLoadBalancer(new AbstractLoadBalancer() {
            @Override
            public Member next() {
                Member[] members = getMembers();
                if (members == null || members.length == 0) {
                    return null;
                }
                return members[0];
            }

            @Override
            public Member nextDataMember() {
                Member[] members = getDataMembers();
                if (members == null || members.length == 0) {
                    return null;
                }
                return members[0];
            }

            @Override
            public boolean canGetNextDataMember() {
                return true;
            }
        });
        client = hecatonFactory.newHecatonClient(config);
        hecatonFactory.newHecatonInstance();
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testTxnRollback() {
        final String queueName = randomString();
        final TransactionContext context = client.newTransactionContext();
        CountDownLatch txnRollbackLatch = new CountDownLatch(1);
        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);

        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });

        try {
            context.beginTransaction();
            assertNotNull(context.getTxnId());
            final TransactionalQueue queue = context.getQueue(queueName);
            queue.offer(randomString());

            server.shutdown();

            context.commitTransaction();
            fail("commit should throw exception!!!");
        } catch (TransactionException e) {
            context.rollbackTransaction();
            txnRollbackLatch.countDown();
        }

        assertOpenEventually(txnRollbackLatch);
        assertOpenEventually(memberRemovedLatch);

        final IQueue<Object> q = client.getQueue(queueName);
        assertNull(q.poll());
        assertEquals(0, q.size());
    }

    @Test
    public void testTxnRollbackOnServerCrash() {
        final String queueName = randomString();
        final TransactionContext context = client.newTransactionContext();
        CountDownLatch txnRollbackLatch = new CountDownLatch(1);
        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);

        context.beginTransaction();

        final TransactionalQueue queue = context.getQueue(queueName);

        queue.offer(randomString());
        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });
        server.getLifecycleService().terminate();

        try {
            context.commitTransaction();
            fail("commit should throw exception !");
        } catch (TransactionException e) {
            context.rollbackTransaction();
            txnRollbackLatch.countDown();
        }

        assertOpenEventually(txnRollbackLatch);
        assertOpenEventually(memberRemovedLatch);

        final IQueue<Object> q = client.getQueue(queueName);
        assertNull(q.poll());
        assertEquals(0, q.size());
    }

    @Test
    public void testRollbackOnTimeout() {
        String name = randomString();
        IQueue<Object> queue = client.getQueue(name);
        queue.offer(randomString());

        TransactionOptions options = new TransactionOptions().setTimeout(3, TimeUnit.SECONDS);
        TransactionContext context = client.newTransactionContext(options);
        context.beginTransaction();
        try {
            try {
                context.getQueue(name).take();
            } catch (InterruptedException e) {
                fail();
            }
            sleepAtLeastSeconds(5);
            context.commitTransaction();
            fail();
        } catch (TransactionException e) {
            context.rollbackTransaction();
        }
        assertEquals("Queue size should be 1", 1, queue.size());
    }
}
