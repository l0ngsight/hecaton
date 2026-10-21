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

package uk.org.hecaton.cp.internal.datastructures.semaphore;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.RaftServiceDataSerializerHook;
import uk.org.hecaton.cp.internal.datastructures.semaphore.proxy.SessionAwareSemaphoreProxy;
import uk.org.hecaton.cp.internal.session.ProxySessionManagerService;
import uk.org.hecaton.cp.internal.session.SessionAwareProxy;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * Tests that acquire, tryAcquire and drainPermits methods
 * properly release the acquired sessions on errors
 * other than SessionExpiredException and WaitKeyCancelledException.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SessionAwareSemaphoreReleaseAcquiredSessionsOnFailureTest extends HecatonRaftTestSupport {

    private HecatonInstance proxyInstance;
    private ProxySessionManagerService sessionManagerService;
    private SessionAwareSemaphoreProxy semaphore;
    private RaftGroupId groupId;

    @Before
    public void setup() {
        HecatonInstance[] instances = newInstances(3);
        String proxyName = "semaphore@group";
        SessionAwareProxy proxy = (SessionAwareProxy) instances[0].getCPSubsystem().getSemaphore(proxyName);
        proxyInstance = getRandomFollowerInstance(instances, proxy.getGroupId());
        semaphore = (SessionAwareSemaphoreProxy) proxyInstance.getCPSubsystem().getSemaphore(proxyName);
        groupId = semaphore.getGroupId();
        sessionManagerService = getNodeEngineImpl(proxyInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "5000");
        return config;
    }

    @Test
    public void testAcquire_shouldReleaseSessionsOnRuntimeError() {
        initSemaphoreAndAcquirePermits(10, 5);
        assertEquals(5, getSessionAcquireCount());
        dropReplicateOperations();
        try {
            semaphore.acquire(5);
            fail("Acquire operation should have been dropped and failed with OperationTimeoutException");
        } catch (OperationTimeoutException ignored) {
        }
        assertEquals(5, getSessionAcquireCount());
    }


    @Test
    public void testTryAcquire_shouldReleaseSessionsOnRuntimeError() {
        initSemaphoreAndAcquirePermits(2, 1);
        assertEquals(1, getSessionAcquireCount());
        dropReplicateOperations();
        try {
            semaphore.tryAcquire(10, TimeUnit.MINUTES);
            fail("Acquire operation should have been dropped and failed with OperationTimeoutException");
        } catch (OperationTimeoutException ignored) {
        }
        assertEquals(1, getSessionAcquireCount());
    }

    @Test
    public void testDrainPermits_shouldReleaseSessionsOnRuntimeError() {
        initSemaphoreAndAcquirePermits(42, 2);
        assertEquals(2, getSessionAcquireCount());
        dropReplicateOperations();
        try {
            semaphore.drainPermits();
            fail("DrainPermits operation should have been dropped and failed with OperationTimeoutException");
        } catch (OperationTimeoutException ignored) {
        }
        assertEquals(2, getSessionAcquireCount());
    }

    private void initSemaphoreAndAcquirePermits(int initialPermits, int acquiredPermits) {
        // Make sure that we have a session id initialized so that the further
        // requests to acquire sessions do not make a remote call.
        semaphore.init(initialPermits);
        semaphore.acquire(acquiredPermits);
    }

    private void dropReplicateOperations() {
        List<Integer> opTypes = Collections.singletonList(RaftServiceDataSerializerHook.DEFAULT_RAFT_GROUP_REPLICATE_OP);
        dropOperationsFrom(proxyInstance, RaftServiceDataSerializerHook.F_ID, opTypes);
    }

    private long getSessionAcquireCount() {
        long sessionId = sessionManagerService.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        return sessionManagerService.getSessionAcquireCount(groupId, sessionId);
    }
}
