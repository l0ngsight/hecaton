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

import uk.org.hecaton.cache.impl.journal.CacheEventJournalBasicTest;
import uk.org.hecaton.client.cache.impl.HecatonClientCachingProvider;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheEventJournalBasicTest extends CacheEventJournalBasicTest {

    private TestHecatonFactory factory;
    private HecatonInstance client;

    @Override
    protected HecatonInstance getRandomInstance() {
        return client;
    }

    @Override
    protected HecatonInstance[] createInstances() {
        factory = new TestHecatonFactory();
        final HecatonInstance[] instances = factory.newInstances(getConfig(), 2);
        client = factory.newHecatonClient();
        return instances;
    }

    @Override
    protected CacheManager createCacheManager() {
        final HecatonClientCachingProvider provider = createClientCachingProvider(client);
        return provider.getCacheManager();
    }

    @After
    public final void terminate() {
        if (factory != null) {
            factory.terminateAll();
        }
    }
}
