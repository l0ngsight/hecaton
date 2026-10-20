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

package uk.org.hecaton.client.executor;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.spi.exception.RetryableIOException;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceExceptionTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }


    @Test(expected = TargetNotMemberException.class)
    public void testSubmitToNonMember() throws Throwable {
        hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        IExecutorService executorService = client.getExecutorService("test");

        HecatonInstance instance2 = hecatonFactory.newHecatonInstance();
        Member member2 = instance2.getCluster().getLocalMember();

        instance2.shutdown();

        try {
            executorService.submitToMember((Serializable & Callable<String>) () -> "test", member2).get();
        } catch (Exception e) {
            throw e.getCause();
        }
    }

    public static class SecondTimeSuccessCallable implements Serializable, Callable {
        private static AtomicInteger runCount = new AtomicInteger();


        @Override
        public Object call() throws Exception {
            if (runCount.incrementAndGet() == 1) {
                throw new RetryableIOException();
            }
            return "SUCCESS";
        }
    }

    @Test
    public void testRetriableIOException() throws Throwable {
        hecatonFactory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        IExecutorService executorService = client.getExecutorService("test");

        assertEquals("SUCCESS", executorService.submit(new SecondTimeSuccessCallable()).get());
    }
}
