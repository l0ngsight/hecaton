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

import uk.org.hecaton.cache.CachePutAllTest;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCachePutAllTest extends CachePutAllTest {

    private TestHecatonFactory clientFactory;
    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory getInstanceFactory(int instanceCount) {
        clientFactory = new TestHecatonFactory();
        return clientFactory;
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    @Override
    protected void onSetup() {
        super.onSetup();
        ClientConfig clientConfig = createClientConfig();
        client = clientFactory.newHecatonClient(clientConfig);
    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        clientFactory.shutdownAll();
        // Client factory is already shutdown at this test's super class (`CachePutAllTest`)
        // because it is returned instance factory from overridden `getInstanceFactory` method.
        client = null;
    }

    @Override
    protected CachingProvider getCachingProvider() {
        return createClientCachingProvider(client);
    }

}
