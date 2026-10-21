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

package uk.org.hecaton.client.cp.internal.session;

import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.session.AbstractProxySessionManager;
import uk.org.hecaton.cp.internal.session.AbstractProxySessionManagerTest;
import uk.org.hecaton.cp.internal.session.SessionAwareProxy;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSessionManagerTest extends AbstractProxySessionManagerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return new TestHecatonFactory();
    }

    @Before
    public void setupClient() {
        TestHecatonFactory f = (TestHecatonFactory) factory;
        client = f.newHecatonClient();
    }

    @Test
    public void sessionHeartbeatsAreStopped_afterClusterRestart() {
        ClientProxySessionManager sessionManager = (ClientProxySessionManager) getSessionManager();
        long sessionId = sessionManager.acquireSession(groupId);

        AtomicInteger heartbeatCounter = new AtomicInteger();
        doAnswer(invocation -> {
            heartbeatCounter.incrementAndGet();
            return invocation.callRealMethod();
        }).when(sessionManager).heartbeat(groupId, sessionId);

        assertTrueEventually(() -> verify(sessionManager, atLeastOnce()).heartbeat(groupId, sessionId));

        Address address1 = members[0].getCPSubsystem().getLocalCPMember().getAddress();
        Address address2 = members[1].getCPSubsystem().getLocalCPMember().getAddress();
        Address address3 = members[2].getCPSubsystem().getLocalCPMember().getAddress();

        members[0].getLifecycleService().terminate();
        members[1].getLifecycleService().terminate();
        members[2].getLifecycleService().terminate();

        Config config = createConfig(3, 3);
        HecatonInstance instance1 = factory.newHecatonInstance(address1, config);
        HecatonInstance instance2 = factory.newHecatonInstance(address2, config);
        HecatonInstance instance3 = factory.newHecatonInstance(address3, config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3);

        assertTrueEventually(() -> verify(sessionManager, times(1)).invalidateSession(groupId, sessionId));
        int heartbeatCount = heartbeatCounter.get();
        assertTrueAllTheTime(() -> verify(sessionManager, times(heartbeatCount)).heartbeat(groupId, sessionId), 3);
    }

    @Test
    public void testClientSessionManagerShutdown() throws ExecutionException, InterruptedException {
        AbstractProxySessionManager sessionManager = getSessionManager();
        SessionProxyImpl proxy = new SessionProxyImpl(sessionManager, groupId);
        proxy.createSession();

        Map<RaftGroupId, InternalCompletableFuture<Object>> futures = sessionManager.shutdown();
        assertEquals(1, futures.size());

        Entry<RaftGroupId, InternalCompletableFuture<Object>> e = futures.entrySet().iterator().next();
        assertEquals(groupId, e.getKey());
        e.getValue().get();

        exception.expect(IllegalStateException.class);
        proxy.createSession();
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    protected AbstractProxySessionManager getSessionManager() {
        return spy((AbstractProxySessionManager) (((HecatonClientProxy) client).client).getProxySessionManager());
    }

    private static class SessionProxyImpl extends SessionAwareProxy {

        SessionProxyImpl(AbstractProxySessionManager sessionManager, RaftGroupId groupId) {
            super(sessionManager, groupId);
        }

        long createSession() {
            return super.acquireSession();
        }
    }
}
