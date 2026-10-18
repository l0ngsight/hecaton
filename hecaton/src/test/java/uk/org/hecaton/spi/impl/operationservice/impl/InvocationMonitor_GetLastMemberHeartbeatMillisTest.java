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

package uk.org.hecaton.spi.impl.operationservice.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationMonitor_GetLastMemberHeartbeatMillisTest extends HecatonTestSupport {

    public static final int CALL_TIMEOUT = 4000;
    private HecatonInstance local;
    private HecatonInstance remote;
    private InvocationMonitor invocationMonitor;
    private Address localAddress;
    private Address remoteAddress;

    @Before
    public void setup() {
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(CALL_TIMEOUT));

        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances(config);
        local = cluster[0];
        localAddress = getAddress(local);
        remote = cluster[1];
        remoteAddress = getAddress(remote);
        invocationMonitor = getOperationService(local).getInvocationMonitor();
    }

    @Test
    public void whenNullAddress() {
        long result = invocationMonitor.getLastMemberHeartbeatMillis(null);

        assertEquals(0, result);
    }

    @Test
    public void whenNonExistingAddress() throws Exception {
        Address address = new Address(localAddress.getHost(), localAddress.getPort() - 1);

        long result = invocationMonitor.getLastMemberHeartbeatMillis(address);

        assertEquals(0, result);
    }

    @Test
    public void whenLocal() {
        final long startMillis = System.currentTimeMillis();

        assertTrueEventually(() -> assertTrue(startMillis + SECONDS.toMillis(5) < invocationMonitor.getLastMemberHeartbeatMillis(localAddress)));
    }

    @Test
    public void whenRemote() {
        final long startMillis = System.currentTimeMillis();

        assertTrueEventually(() -> assertTrue(startMillis + SECONDS.toMillis(5) < invocationMonitor.getLastMemberHeartbeatMillis(remoteAddress)));
    }

    @Test
    public void whenMemberDies_lastHeartbeatRemoved() {
        // trigger the sending of heartbeats
        DummyOperation op = new DummyOperation().setDelayMillis(CALL_TIMEOUT * 2);

        remote.shutdown();

        assertTrueEventually(() -> assertEquals(0, invocationMonitor.getLastMemberHeartbeatMillis(remoteAddress)));
    }
}
