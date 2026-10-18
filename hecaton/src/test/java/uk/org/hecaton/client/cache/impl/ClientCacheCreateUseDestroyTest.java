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

package uk.org.hecaton.client.cache.impl;

import uk.org.hecaton.cache.impl.CacheCreateUseDestroyTest;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.client.cache.jsr.JsrClientTestUtil;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static uk.org.hecaton.cache.HecatonCachingProvider.propertiesByInstanceItself;
import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientCacheCreateUseDestroyTest extends CacheCreateUseDestroyTest {

    private TestHecatonFactory factory;

    @Before
    @Override
    public void setup() {
        assumptions();
        JsrClientTestUtil.setup();

        factory = new TestHecatonFactory();
        HecatonInstance member = factory.newHecatonInstance(getConfig());
        CachingProvider provider = Caching.getCachingProvider();
        defaultCacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(factory.newHecatonClient()));
        cacheService = getNode(member).getNodeEngine().getService(ICacheService.SERVICE_NAME);
        CacheEntryListenerFactory.listener = null;
    }

    @After
    @Override
    public void tearDown() {
        if (factory != null) {
            factory.terminateAll();
        }
        JsrClientTestUtil.cleanup();
    }
}
