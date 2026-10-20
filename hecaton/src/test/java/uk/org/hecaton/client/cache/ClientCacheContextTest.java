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

package uk.org.hecaton.client.cache;

import uk.org.hecaton.cache.CacheContextTest;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheContextTest extends CacheContextTest {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @Before
    @Override
    public void setup() {
        hecatonInstance1 = factory.newHecatonInstance();
        hecatonInstance2 = factory.newHecatonInstance();

        driverInstance = factory.newHecatonClient();
        provider = createClientCachingProvider(driverInstance);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
        driverInstance.shutdown();
        hecatonInstance1.shutdown();
        hecatonInstance2.shutdown();
    }

    @Override
    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterDeregister() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.DEREGISTER);
    }

    @Override
    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterShutdown() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.SHUTDOWN);
    }

    @Override
    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterTerminate() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.TERMINATE);
    }
}
