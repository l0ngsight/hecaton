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
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_timeoutSlowTest extends HecatonTestSupport {

    @Test
    public void testOperationTimeoutForLongRunningRemoteOperation() throws Exception {
        // this timeout makes the test slow. however we cannot shorten it too much
        // as otherwise it becomes to sensitive on environmental hiccups
        int callTimeoutMillis = 15000;
        Config config = new Config().setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(callTimeoutMillis));

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance hz1 = factory.newHecatonInstance(config);
        HecatonInstance hz2 = factory.newHecatonInstance(config);

        // invoke on the "remote" member
        Address remoteAddress = getNode(hz2).getThisAddress();
        OperationService operationService = getNode(hz1).getNodeEngine().getOperationService();
        InternalCompletableFuture<Boolean> future = operationService
                .invokeOnTarget(null, new OperationServiceImpl_timeoutTest.SleepingOperation(callTimeoutMillis * 5), remoteAddress);

        // wait more than operation timeout
        sleepAtLeastMillis(callTimeoutMillis * 3);
        assertTrue(future.get());
    }
}
