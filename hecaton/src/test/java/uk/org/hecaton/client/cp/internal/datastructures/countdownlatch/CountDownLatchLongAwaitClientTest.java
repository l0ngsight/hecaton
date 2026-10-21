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

package uk.org.hecaton.client.cp.internal.datastructures.countdownlatch;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.ICountDownLatch;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import uk.org.hecaton.spi.properties.ClusterProperty;
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

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CountDownLatchLongAwaitClientTest extends HecatonRaftTestSupport {

    private HecatonInstance[] instances;
    private String objectName = "latch";
    private String proxyName = objectName + "@group1";
    private int groupSize = 3;
    private CPGroupId groupId;
    private final long callTimeoutSeconds = 15;
    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return new TestHecatonFactory();
    }

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        TestHecatonFactory f = (TestHecatonFactory) factory;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(callTimeoutSeconds));
        client = f.newHecatonClient(clientConfig);
        groupId = ((CountDownLatchProxy) client.getCPSubsystem().getCountDownLatch(proxyName)).getGroupId();
    }

    @Test
    public void when_awaitDurationIsLongerThanOperationTimeout_then_invocationFromClientWaits() throws ExecutionException, InterruptedException {
        ICountDownLatch latch = client.getCPSubsystem().getCountDownLatch(proxyName);
        HecatonInstance instance = getLeaderInstance(instances, groupId);
        latch.trySetCount(1);

        Future<Boolean> f = spawn(() -> latch.await(5, TimeUnit.MINUTES));

        assertTrueEventually(() -> {
            CountDownLatchService service = getNodeEngineImpl(instance).getService(CountDownLatchService.SERVICE_NAME);
            assertFalse(service.getLiveOperations(groupId).isEmpty());
        });

        assertTrueAllTheTime(() -> {
            CountDownLatchService service = getNodeEngineImpl(instance).getService(CountDownLatchService.SERVICE_NAME);
            assertFalse(service.getLiveOperations(groupId).isEmpty());
        }, callTimeoutSeconds + 5);

        latch.countDown();

        assertCompletesEventually(f);
        assertTrue(f.get());
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        String callTimeoutStr = String.valueOf(SECONDS.toMillis(callTimeoutSeconds));
        return super.createConfig(cpNodeCount, groupSize)
                    .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), callTimeoutStr);
    }

}
