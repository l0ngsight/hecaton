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

import com.atomikos.datasource.xa.XID;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.transaction.TransactionalMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.transaction.HecatonXAResource;
import uk.org.hecaton.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class ClientXAStressTest extends HecatonTestSupport {

    private HecatonInstance instance;
    private HecatonInstance secondInstance;
    private HecatonInstance client;
    private HecatonInstance secondClient;

    private HecatonXAResource xaResource;
    private HecatonXAResource secondXaResource;
    private HecatonXAResource instanceXaResource;

    private static Xid createXid() {
        return new XID(randomString(), "test");
    }

    @Before
    public void setUp() {
        instance = Hecaton.newHecatonInstance();
        secondInstance = Hecaton.newHecatonInstance();
        instanceXaResource = instance.getXAResource();

        client = HecatonClient.newHecatonClient();
        secondClient = HecatonClient.newHecatonClient();
        xaResource = client.getXAResource();
        secondXaResource = secondClient.getXAResource();
    }

    @After
    public void tearDown() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    public void testCommitConcurrently() {
        int count = 10000;
        String name = randomString();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        ExecutorService executorServiceForCommit = Executors.newFixedThreadPool(5);
        for (int i = 0; i < count; i++) {
            XATransactionRunnable runnable = new XATransactionRunnable(xaResource, name, executorServiceForCommit, i);
            executorService.execute(runnable);
        }
        IMap<Object, Object> map = client.getMap(name);
        assertSizeEventually(count, map);
        executorService.shutdown();
        executorServiceForCommit.shutdown();
    }

    static class XATransactionRunnable implements Runnable {

        HecatonXAResource xaResource;

        String name;

        ExecutorService executorServiceForCommit;

        int i;

        XATransactionRunnable(HecatonXAResource xaResource, String name,
                                     ExecutorService executorServiceForCommit, int i) {
            this.xaResource = xaResource;
            this.name = name;
            this.executorServiceForCommit = executorServiceForCommit;
            this.i = i;
        }

        @Override
        public void run() {
            try {
                final Xid xid = createXid();
                xaResource.start(xid, XAResource.TMNOFLAGS);
                TransactionContext context = xaResource.getTransactionContext();
                TransactionalMap<Object, Object> map = context.getMap(name);
                map.put(i, i);
                xaResource.end(xid, XAResource.TMSUCCESS);
                executorServiceForCommit.execute(() -> {
                    try {
                        xaResource.commit(xid, true);
                    } catch (XAException e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
