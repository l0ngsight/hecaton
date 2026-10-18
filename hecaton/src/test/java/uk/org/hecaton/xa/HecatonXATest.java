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

package uk.org.hecaton.xa;

import com.atomikos.icatch.jta.UserTransactionManager;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.HecatonXAResource;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionalMap;
import uk.org.hecaton.transaction.impl.xa.SerializableXID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class HecatonXATest extends HecatonTestSupport {

    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final ILogger LOGGER = Logger.getLogger(HecatonXATest.class);

    private UserTransactionManager tm;

    public void cleanAtomikosLogs() {
        try {
            File currentDir = new File(".");
            final File[] tmLogs = currentDir.listFiles((dir, name) -> name.endsWith(".epoch") || name.startsWith("tmlog"));
            for (File tmLog : tmLogs) {
                tmLog.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void init() throws SystemException {
        cleanAtomikosLogs();
        tm = new UserTransactionManager();
        tm.setTransactionTimeout(60);
    }

    @After
    public void cleanup() {
        tm.close();
        cleanAtomikosLogs();
    }

    @Test
    public void testRollback() throws Exception {
        HecatonInstance instance = createHecatonInstance();
        HecatonXAResource xaResource = instance.getXAResource();

        tm.begin();
        Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);
        TransactionContext context = xaResource.getTransactionContext();
        boolean error = false;
        try {
            TransactionalMap<String, String> map = context.getMap("m");
            map.put("key", "value");
            throw new RuntimeException("Exception for rolling back");
        } catch (Exception e) {
            error = true;
        } finally {
            close(error, xaResource);
        }

        assertNull(instance.getMap("m").get("key"));
    }

    @Test
    public void testRecovery() throws Exception {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        HecatonInstance instance1 = factory.newHecatonInstance();
        HecatonInstance instance2 = factory.newHecatonInstance();
        HecatonInstance instance3 = factory.newHecatonInstance();

        HecatonXAResource xaResource = instance1.getXAResource();
        Xid myXid = new SerializableXID(42, "globalTransactionId".getBytes(), "branchQualifier".getBytes());
        xaResource.start(myXid, 0);
        TransactionContext context1 = xaResource.getTransactionContext();
        TransactionalMap<Object, Object> map = context1.getMap("map");
        map.put("key", "value");
        xaResource.prepare(myXid);
        instance1.shutdown();

        instance1 = factory.newHecatonInstance();
        xaResource = instance1.getXAResource();
        Xid[] recovered = xaResource.recover(0);
        for (Xid xid : recovered) {
            xaResource.commit(xid, false);
        }

        assertEquals("value", instance2.getMap("map").get("key"));
    }

    /**
     * Start two nodes.
     * One node in a new thread prepares tx and shutdowns. Check if remaining node can recover tx or not.
     */
    @Test
    public void testRecovery_singleInstanceRemaining() throws XAException {
        final CountDownLatch nodeShutdownLatch = new CountDownLatch(1);
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = factory.newHecatonInstance();
        HecatonInstance instance2 = factory.newHecatonInstance();

        // 1. Start TX from a new thread.
        startTX(instance1, nodeShutdownLatch);

        waitNodeToShutDown(nodeShutdownLatch);

        // 2. Try to recover TX from other node.
        recoverTX(instance2);

        IMap<String, String> map = instance2.getMap("map");
        String actualValue = map.get("key");
        assertEquals("value", actualValue);
    }

    private void waitNodeToShutDown(CountDownLatch nodeShutdownLatch) {
        assertOpenEventually(nodeShutdownLatch);
    }

    private void startTX(final HecatonInstance instance, final CountDownLatch nodeShutdownLatch) {
        new Thread(() -> {
            try {
                HecatonXAResource xaResource = instance.getXAResource();
                Xid xid = new SerializableXID(42, "globalTransactionId".getBytes(), "branchQualifier".getBytes());
                xaResource.start(xid, XAResource.TMNOFLAGS);
                TransactionContext context = xaResource.getTransactionContext();
                final TransactionalMap<Object, Object> map = context.getMap("map");
                map.put("key", "value");
                xaResource.prepare(xid);

                instance.shutdown();

                nodeShutdownLatch.countDown();

            } catch (XAException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void recoverTX(HecatonInstance instance) throws XAException {
        HecatonXAResource xaResource = instance.getXAResource();
        final Xid[] recovered = xaResource.recover(XAResource.TMNOFLAGS);
        for (Xid xid : recovered) {
            xaResource.commit(xid, false);
        }
    }

    @Test
    public void testIsSame() throws Exception {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = factory.newHecatonInstance();
        HecatonInstance instance2 = factory.newHecatonInstance();
        HecatonXAResource resource1 = instance1.getXAResource();
        HecatonXAResource resource2 = instance2.getXAResource();
        assertTrue(resource1.isSameRM(resource2));
    }

    @Test
    public void testParallel() throws Exception {
        final HecatonInstance instance = createHecatonInstance();
        // this is needed due to a racy bug in atomikos
        txn(instance);
        //Perform 100 transactions with 5 threads
        int size = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        final CountDownLatch latch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            executorService.execute(() -> {
                try {
                    txn(instance);
                } catch (Exception e) {
                    LOGGER.severe("Exception during txn", e);
                } finally {
                    latch.countDown();
                }
            });
        }
        assertOpenEventually(latch, 60);
        final IMap map = instance.getMap("m");
        for (int i = 0; i < 10; i++) {
            assertFalse(map.isLocked(i));
        }
        executorService.shutdownNow();
    }

    @Test
    public void testSequential() throws Exception {
        HecatonInstance instance = createHecatonInstance();
        int count = 100;
        for (int i = 0; i < count; i++) {
            txn(instance);
        }
    }

    private void txn(HecatonInstance instance) throws Exception {
        HecatonXAResource xaResource = instance.getXAResource();

        tm.begin();
        Transaction transaction = tm.getTransaction();
        transaction.enlistResource(xaResource);

        boolean error = false;
        try {
            TransactionContext context = xaResource.getTransactionContext();
            TransactionalMap<Integer, String> map = context.getMap("m");
            map.put(RANDOM.nextInt(10), "value");
        } catch (Exception e) {
            LOGGER.severe("Exception during transaction", e);
            error = true;
        } finally {
            // commit or rollback
            close(error, xaResource);
        }
    }

    private void close(boolean error, XAResource... xaResource) throws Exception {

        int flag = XAResource.TMSUCCESS;

        // get the current tx
        Transaction tx = tm.getTransaction();
        // closeConnection
        if (error) {
            flag = XAResource.TMFAIL;
        }
        for (XAResource resource : xaResource) {
            tx.delistResource(resource, flag);
        }

        if (error) {
            tm.rollback();
        } else {
            tm.commit();
        }
    }
}
