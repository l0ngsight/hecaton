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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.transaction.TransactionalSet;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnSetTest {
    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance client;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        hecatonFactory.newHecatonInstance();
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient();
    }


    @Test
    public void testAdd_withinTxn() {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = client.getSet(setName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.add(element));
        assertEquals(1, txnSet.size());

        context.commitTransaction();
    }

    @Test
    public void testSetSizeAfterAdd_withinTxn() {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = client.getSet(setName);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.add(element);

        context.commitTransaction();

        assertEquals(1, set.size());
    }

    @Test
    public void testRemove_withinTxn() {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = client.getSet(setName);
        set.add(element);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.remove(element));
        assertFalse(txnSet.remove("NOT_THERE"));

        context.commitTransaction();
    }

    @Test
    public void testSetSizeAfterRemove_withinTxn() {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = client.getSet(setName);
        set.add(element);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.remove(element);

        context.commitTransaction();

        assertEquals(0, set.size());
    }

    @Test
    public void testAddDuplicateElement_withinTxn() {
        final String element = "item1";
        final String setName = randomString();

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.add(element));
        assertFalse(txnSet.add(element));
        context.commitTransaction();
        assertEquals(1, client.getSet(setName).size());
    }

    @Test
    public void testAddExistingElement_withinTxn() {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = client.getSet(setName);
        set.add(element);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertFalse(txnSet.add(element));

        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetSizeAfterAddingDuplicateElement_withinTxn() {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = client.getSet(setName);
        set.add(element);

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.add(element);
        context.commitTransaction();

        assertEquals(1, set.size());
    }

    @Test
    public void testAddRollBack() {
        final String setName = randomString();
        final ISet set = client.getSet(setName);

        set.add("item1");

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> setTxn = context.getSet(setName);
        setTxn.add("item2");
        context.rollbackTransaction();

        assertEquals(1, set.size());
    }
}
