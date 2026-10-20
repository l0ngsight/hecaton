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

package uk.org.hecaton.durableexecutor;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.executor.ExecutorServiceTestSupport;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableSpecificSetupTest extends ExecutorServiceTestSupport {

    @Test
    public void managedContext_mustInitializeRunnable() throws Exception {
        final AtomicBoolean initialized = new AtomicBoolean();
        Config config = smallInstanceConfig()
                .addDurableExecutorConfig(new DurableExecutorConfig("test").setPoolSize(1))
                .setManagedContext(obj -> {
                    if (obj instanceof RunnableWithManagedContext) {
                        initialized.set(true);
                    }
                    return obj;
                });
        DurableExecutorService executor = createHecatonInstance(config).getDurableExecutorService("test");
        executor.submit(new RunnableWithManagedContext()).get();
        assertTrue("The task should have been initialized by the ManagedContext", initialized.get());
    }

    @Test
    public void operationTimeoutConfigProp() throws Exception {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        Config config = smallInstanceConfig();
        int timeoutSeconds = 3;
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf(SECONDS.toMillis(timeoutSeconds)));
        HecatonInstance hz1 = factory.newHecatonInstance(config);
        HecatonInstance hz2 = factory.newHecatonInstance(config);
        String key = generateKeyOwnedBy(hz2);
        DurableExecutorService executor = hz1.getDurableExecutorService(randomString());
        Future<Boolean> future = executor.submitToKeyOwner(new SleepingTask(3 * timeoutSeconds), key);
        Boolean result = future.get(1, MINUTES);
        assertTrue(result);
    }

    static class RunnableWithManagedContext implements Runnable, Serializable {
        @Override
        public void run() {
        }
    }
}
