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

package uk.org.hecaton.collection.impl.txnlist;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.transaction.TransactionalList;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionListTest extends HecatonTestSupport {

    @Test
    public void testSingleListAtomicity() throws ExecutionException, InterruptedException {
        final int itemCount = 200;
        final HecatonInstance instance = createHecatonInstance();
        final String name = randomString();

        Future<Integer> f = spawn(() -> {
            IList<Object> set = instance.getList(name);
            while (!set.remove("item-1")) {
            }
            return set.size();
        });

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();

        TransactionalList<Object> set = context.getList(name);
        for (int i = 0; i < itemCount; i++) {
            set.add("item-" + i);
        }
        context.commitTransaction();

        int size = f.get();
        assertEquals(itemCount - 1, size);
    }

    @Test
    public void testOrder_WhenMultipleConcurrentTransactionRollback() throws InterruptedException {
        final HecatonInstance instance = createHecatonInstance();
        final String name = randomString();
        IList<Integer> list = instance.getList(name);
        list.add(1);
        list.add(2);
        list.add(3);

        TransactionContext firstContext = instance.newTransactionContext();
        firstContext.beginTransaction();
        firstContext.getList(name).remove(1);

        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            TransactionContext secondContext = instance.newTransactionContext();
            secondContext.beginTransaction();
            secondContext.getList(name).remove(2);
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            secondContext.rollbackTransaction();
        });
        thread.start();
        firstContext.rollbackTransaction();
        latch.countDown();
        thread.join();

        assertEquals(1, list.get(0).intValue());
        assertEquals(2, list.get(1).intValue());
        assertEquals(3, list.get(2).intValue());

    }

    @Test
    public void testAdd() {
        HecatonInstance instance = createHecatonInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        TransactionContext context = instance.newTransactionContext();
        try {
            context.beginTransaction();
            TransactionalList<Object> txnList = context.getList(name);
            assertTrue(txnList.add(item));
            context.commitTransaction();
        } catch (Exception e) {
            fail(e.getMessage());
            context.rollbackTransaction();
        }
        assertEquals(1, list.size());
        assertEquals(item, list.get(0));
    }

    @Test
    public void testRemove() {
        HecatonInstance instance = createHecatonInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);
        TransactionContext context = instance.newTransactionContext();
        try {
            context.beginTransaction();
            TransactionalList<Object> txnList = context.getList(name);
            assertTrue(txnList.remove(item));
            context.commitTransaction();
        } catch (Exception e) {
            fail(e.getMessage());
            context.rollbackTransaction();
        }
        assertEquals(0, list.size());
    }

    @Test
    public void testRemove_withNotContainedItem() {
        HecatonInstance instance = createHecatonInstance();
        String name = randomString();
        String item = randomString();
        String notContainedItem = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);
        TransactionContext context = instance.newTransactionContext();
        try {
            context.beginTransaction();
            TransactionalList<Object> txnList = context.getList(name);
            assertFalse(txnList.remove(notContainedItem));
            context.commitTransaction();
        } catch (Exception e) {
            fail(e.getMessage());
            context.rollbackTransaction();
        }
        assertEquals(1, list.size());
    }

    @Test
    public void testMigrationSerializationNotFails_whenTransactionsAreUsed() {
        Config config = new Config();
        config.setProperty("hecaton.partition.count", "2");
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = factory.newHecatonInstance(config);
        String listName = randomString();
        TransactionContext tr = instance1.newTransactionContext();
        tr.beginTransaction();
        TransactionalList<Object> list = tr.getList(listName);
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        tr.commitTransaction();
        HecatonInstance instance2 = factory.newHecatonInstance(config);
        Member owner = instance1.getPartitionService().getPartition(listName).getOwner();
        HecatonInstance aliveInstance;
        if (instance1.getCluster().getLocalMember().equals(owner)) {
            instance1.shutdown();
            aliveInstance = instance2;
        } else {
            instance2.shutdown();
            aliveInstance = instance1;
        }
        IList<Object> l = aliveInstance.getList(listName);

        for (int i = 0; i < 10; i++) {
            assertEquals(i, l.get(i));
        }
    }

    @Test
    public void transactionShouldBeRolledBack_whenInitiatorTerminatesBeforeCommit() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance master = factory.newHecatonInstance();
        HecatonInstance instance = factory.newHecatonInstance();
        warmUpPartitions(instance);

        String name = generateKeyOwnedBy(master);
        IList<Integer> list = master.getList(name);
        list.add(1);

        waitAllForSafeState(master, instance);

        TransactionOptions options =
                new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);

        TransactionContext context = master.newTransactionContext(options);
        context.beginTransaction();
        TransactionalList<Integer> txList = context.getList(name);
        txList.remove(1);

        master.getLifecycleService().terminate();

        final IList<Integer> list2 = instance.getList(name);

        assertTrueEventually(() -> assertEquals(1, list2.size()));

        assertTrueAllTheTime(() -> assertEquals(1, list2.size()), 3);
    }

}
