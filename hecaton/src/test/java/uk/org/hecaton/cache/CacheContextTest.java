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

package uk.org.hecaton.cache;

import uk.org.hecaton.cache.impl.CacheContext;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheContextTest extends HecatonTestSupport {

    private static final String CACHE_NAME = "MyCache";
    private static final String CACHE_NAME_WITH_PREFIX = "/hz/" + CACHE_NAME;

    protected HecatonInstance driverInstance;
    protected HecatonInstance hecatonInstance1;
    protected HecatonInstance hecatonInstance2;
    protected CachingProvider provider;

    @Before
    public void setup() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        hecatonInstance1 = factory.newHecatonInstance();
        hecatonInstance2 = factory.newHecatonInstance();

        driverInstance = hecatonInstance1;
        provider = createServerCachingProvider(driverInstance);
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterDeregister() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.DEREGISTER);
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterShutdown() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.SHUTDOWN);
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterTerminate() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.TERMINATE);
    }

    protected void cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType decreaseType) {
        CacheManager cacheManager = provider.getCacheManager();
        CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfig
                = new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(new TestListener()), null, true, true);
        CompleteConfiguration<String, String> cacheConfig = new MutableConfiguration<>();
        Cache<String, String> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.registerCacheEntryListener(cacheEntryListenerConfig);

        final CacheService cacheService1 = getCacheService(hecatonInstance1);
        final CacheService cacheService2 = getCacheService(hecatonInstance2);

        assertTrueEventually(() -> assertNotNull(cacheService1.getCacheContext(CACHE_NAME_WITH_PREFIX)));

        assertTrueEventually(() -> assertNotNull(cacheService2.getCacheContext(CACHE_NAME_WITH_PREFIX)));

        final CacheContext cacheContext1 = cacheService1.getCacheContext(CACHE_NAME_WITH_PREFIX);
        final CacheContext cacheContext2 = cacheService2.getCacheContext(CACHE_NAME_WITH_PREFIX);

        assertEqualsEventually(cacheContext1::getCacheEntryListenerCount, 1);
        assertEqualsEventually(cacheContext2::getCacheEntryListenerCount, 1);

        switch (decreaseType) {
            case DEREGISTER:
                cache.deregisterCacheEntryListener(cacheEntryListenerConfig);
                break;
            case SHUTDOWN:
                driverInstance.getLifecycleService().shutdown();
                break;
            case TERMINATE:
                driverInstance.getLifecycleService().terminate();
                break;
            default:
                throw new IllegalArgumentException("Unsupported decrease type: " + decreaseType);
        }

        assertEqualsEventually(cacheContext1::getCacheEntryListenerCount, 0);
        assertEqualsEventually(cacheContext2::getCacheEntryListenerCount, 0);
    }

    private CacheService getCacheService(HecatonInstance instance) {
        return getNodeEngineImpl(instance).getService(CacheService.SERVICE_NAME);
    }

    protected enum DecreaseType {
        DEREGISTER,
        SHUTDOWN,
        TERMINATE
    }

    public static class TestListener implements CacheEntryCreatedListener<String, String>, Serializable {

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {
        }
    }
}
