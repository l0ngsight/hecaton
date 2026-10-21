/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.cp.internal.datastructures.semaphore;

import uk.org.hecaton.client.cp.internal.session.ClientProxySessionManager;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.datastructures.semaphore.SessionAwareSemaphoreBasicTest;
import uk.org.hecaton.cp.internal.session.AbstractProxySessionManager;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SessionAwareSemaphoreClientBasicTest extends SessionAwareSemaphoreBasicTest {

    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return new TestHecatonFactory();
    }

    @Override
    protected HecatonInstance[] createInstances() {
        HecatonInstance[] instances = super.createInstances();
        TestHecatonFactory f = (TestHecatonFactory) factory;
        client = f.newHecatonClient();
        return instances;
    }

    @Override
    protected HecatonInstance getProxyInstance() {
        return client;
    }

    @Override
    protected AbstractProxySessionManager getSessionManager(HecatonInstance instance) {
        return (AbstractProxySessionManager) (((HecatonClientProxy) instance).client).getProxySessionManager();
    }

    @Override
    protected RaftGroupId getGroupId(ISemaphore semaphore) {
        return (RaftGroupId) ((SessionAwareSemaphoreProxy) semaphore).getGroupId();
    }


    @Test
    public void testDrain_ReleasesSessionProperly() throws InterruptedException {
        int permits = 20;

        assertTrue(semaphore.init(permits));
        final int drainPermits = semaphore.drainPermits();

        HecatonClientProxy clientProxy = (HecatonClientProxy) client;
        ClientProxySessionManager proxySessionManager = (ClientProxySessionManager) clientProxy.client.getProxySessionManager();
        SessionAwareSemaphoreProxy proxy = (SessionAwareSemaphoreProxy) semaphore;
        RaftGroupId groupId = (RaftGroupId) proxy.getGroupId();
        final long session = proxySessionManager.getSession(groupId);
        assertEquals(drainPermits, proxySessionManager.getSessionAcquireCount(groupId, session));
    }

}
