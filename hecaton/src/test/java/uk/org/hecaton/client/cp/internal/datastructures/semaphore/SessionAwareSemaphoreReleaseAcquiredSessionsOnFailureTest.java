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
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that acquire, tryAcquire and drainPermits methods
 * properly release the acquired sessions on errors
 * other than SessionExpiredException and WaitKeyCancelledException.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SessionAwareSemaphoreReleaseAcquiredSessionsOnFailureTest extends HecatonRaftTestSupport {

    private SessionAwareSemaphoreProxy semaphore;
    private ClientProxySessionManager sessionManager;
    private RaftGroupId groupId;

    @Before
    public void setup() {
        newInstances(3);
        String proxyName = "semaphore@group";
        HecatonInstance client = ((TestHecatonFactory) factory).newHecatonClient();
        sessionManager = (ClientProxySessionManager) (((HecatonClientProxy) client).client).getProxySessionManager();
        semaphore = (SessionAwareSemaphoreProxy) client.getCPSubsystem().getSemaphore(proxyName);
        groupId = (RaftGroupId) semaphore.getGroupId();
    }

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return new TestHecatonFactory();
    }

    @Test
    public void testAcquire_shouldReleaseSessionsOnRuntimeError() throws InterruptedException {
        initSemaphoreAndAcquirePermits(10, 5);
        assertEquals(5, getSessionAcquireCount());
        Future future = spawn(() -> {
            Thread.currentThread().interrupt();
            // Make the semaphore block, so that Thread.interrupt() can be detected
            semaphore.acquire(6);
        });

        try {
            future.get();
            fail("Acquire request should have been completed with InterruptedException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof HecatonException);
            assertTrue(e.getCause().getCause() instanceof InterruptedException);
        }
        assertEquals(5, getSessionAcquireCount());
    }

    @Test
    public void testTryAcquire_shouldReleaseSessionsOnRuntimeError() throws InterruptedException {
        initSemaphoreAndAcquirePermits(2, 1);
        assertEquals(1, getSessionAcquireCount());
        Future future = spawn(() -> {
            Thread.currentThread().interrupt();
            // Make the semaphore block, so that Thread.interrupt() can be detected
            semaphore.tryAcquire(2, 10, TimeUnit.MINUTES);
        });

        try {
            future.get();
            fail("TryAcquire request should have been completed with InterruptedException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof HecatonException);
            assertTrue(e.getCause().getCause() instanceof InterruptedException);
        }
        assertEquals(1, getSessionAcquireCount());
    }

    @Test
    public void testDrainPermits_shouldReleaseSessionsOnRuntimeError() throws InterruptedException {
        initSemaphoreAndAcquirePermits(42, 2);
        assertEquals(2, getSessionAcquireCount());
        Future future = spawn(() -> {
            Thread.currentThread().interrupt();
            semaphore.drainPermits();
        });

        try {
            future.get();
            fail("DrainPermits request should have been completed with InterruptedException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof HecatonException);
            assertTrue(e.getCause().getCause() instanceof InterruptedException);
        }
        assertEquals(2, getSessionAcquireCount());
    }

    private void initSemaphoreAndAcquirePermits(int initialPermits, int acquiredPermits) {
        // Make sure that we have a session id initialized so that the further
        // requests to acquire sessions do not make a remote call.
        semaphore.init(initialPermits);
        semaphore.acquire(acquiredPermits);
    }

    private long getSessionAcquireCount() {
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        return sessionManager.getSessionAcquireCount(groupId, sessionId);
    }
}
