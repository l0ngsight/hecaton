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
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.transaction.TransactionalList;
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

/**
 * @author ali 6/11/13
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnListTest {
    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance client;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient();
    }

    @Test
    public void testAddRemove() {
        String listName = randomString();
        final IList<String> l = client.getList(listName);
        l.add("item1");

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalList<Object> list = context.getList(listName);
        assertTrue(list.add("item2"));
        assertEquals(2, list.size());
        assertEquals(1, l.size());
        assertFalse(list.remove("item3"));
        assertTrue(list.remove("item1"));

        context.commitTransaction();

        assertEquals(1, l.size());
    }

    @Test
    public void testAddAndRoleBack() {
        final String listName = randomString();
        final IList<String> l = client.getList(listName);
        l.add("item1");

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalList<Object> list = context.getList(listName);
        list.add("item2");
        context.rollbackTransaction();

        assertEquals(1, l.size());
    }
}
