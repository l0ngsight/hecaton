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

package uk.org.hecaton.spi.tenantcontrol;

import uk.org.hecaton.cache.CacheUtil;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.cache.CacheTestSupport.getCacheService;
import static uk.org.hecaton.cache.CacheTestSupport.getTenantControl;
import static uk.org.hecaton.cache.HecatonCachingProvider.propertiesByInstanceItself;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class TenantControlTest extends TenantControlTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(TenantControlTest.class);

    @Parameter
    public boolean hasTenantControl;

    private String cacheName;

    @Parameters(name = "tenantControl: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Before
    public void setup() {
        cacheName = randomName();
        initState();
    }

    private Config getNewConfig() {
        return newConfig(hasTenantControl, TenantControlTest.class.getClassLoader());
    }

    @Test
    public void testTenantControl_whenCacheCreatedViaCacheManager() {
        HecatonInstance hz = createHecatonInstance(getNewConfig());

        CachingProvider provider = createServerCachingProvider(hz);
        CacheManager cacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(hz));
        @SuppressWarnings("unused")
        Cache<?, ?> cache = cacheManager.createCache(cacheName, new CacheConfig<>());

        assertCacheTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_whenCacheObtainedViaCacheManager() {
        HecatonInstance hz = createHecatonInstance(getNewConfig());

        CachingProvider provider = createServerCachingProvider(hz);
        CacheManager cacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(hz));

        @SuppressWarnings("unused")
        Cache<?, ?> cache = cacheManager.getCache(cacheName);

        assertCacheTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_whenCacheObtainedAsDistributedObject() {
        HecatonInstance hz = createHecatonInstance(getNewConfig());

        @SuppressWarnings("unused")
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        assertCacheTenantControlCreated(hz);
    }

    @Test
    public void testTenantControl_executionBeforeAfterOps() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HecatonInstance hz = createHecatonInstance(getNewConfig());
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        cache.put(1, 1);
        cache.get(1);
        cache.getAndPut(1, 2);

        cache.destroy();

        assertNotNull(savedTenant.get());
        // expecting tenant context is created & closed 4 times:
        // CachePutOperation
        // AbstractCacheRecordStore.getExpiryPolicy
        // CacheGetOperation
        // CachePutOperation
        // these operations don't require tenant control:
        // AddCacheConfigOperation
        // TenantControlReplicationOperation
        assertEquals(4, setTenantCount.get());
        assertEqualsEventually(4, closeTenantCount);
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
        // thread context should be cleared at least twice.
        // in most cases it would be three times, but there is a case when the structure
        // gets destroyed before operation completes, in which case it's valid
        // to only have thread context cleared only twice
        assertTrueEventually(() -> assertThat(clearedThreadInfoCount.get())
                .as("thread context not cleared enough times")
                .isGreaterThanOrEqualTo(2), 10);
    }

    @Test
    public void testDestroyEventContext_destroyRemovesTenantControl() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HecatonInstance hz = createHecatonInstance(getNewConfig());
        ICache<Integer, Integer> cache = hz.getCacheManager().getCache(cacheName);

        cache.put(1, 1);
        cache.get(1);
        cache.getAndPut(1, 2);

        destroyEventContext.get().tenantUnavailable();

        assertInstanceOf(CountingTenantControl.class, getTenantControl(hz, cache));
    }

    @Test
    public void basicMapTest() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HecatonInstance hz = createHecatonInstance(getNewConfig());
        IMap<String, Integer> map = hz.getMap(randomName());
        map.addEntryListener((EntryAddedListener<String, Integer>)
                (EntryEvent<String, Integer> event) -> LOGGER.info("Added: " + event.getValue()), true);
        map.put("oneKey", 1);
        map.destroy();
        assertNotNull(savedTenant.get());
        // only PutOperation requires and establishes tenant context
        // event listener registration does not and map destroy
        // operation does not need tenant context
        assertEquals(1, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
    }

    @Test
    public void basicSetTest() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HecatonInstance hz = createHecatonInstance(getNewConfig());
        ISet<Integer> set = hz.getSet(randomName());
        set.add(1);
        set.add(1);
        set.remove(1);
        set.clear();
        set.destroy();
        assertNotNull(savedTenant.get());
        // set tenant is called twice for add, once for remove and clear
        assertEquals(4, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
    }

    @Test
    public void basicQueueTest() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HecatonInstance hz = createHecatonInstance(getNewConfig());
        IQueue<Integer> q = hz.getQueue(randomName());
        q.add(1);
        q.add(1);
        q.remove(1);
        q.clear();
        q.destroy();
        assertNotNull(savedTenant.get());
        // set tenant is called twice for add, once for remove and clear
        assertEquals(4, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
    }

    @Test
    public void basicListTest() {
        assumeTrue("Requires CountingTenantControl explicitly configured", hasTenantControl);
        HecatonInstance hz = createHecatonInstance(getNewConfig());
        IList<Integer> l = hz.getList(randomName());
        l.add(1);
        l.add(1);
        l.remove(1);
        l.clear();
        l.destroy();
        assertNotNull(savedTenant.get());
        // set tenant is called twice for add, once for remove and clear
        assertEquals(4, setTenantCount.get());
        assertEquals(1, registerTenantCount.get());
        assertEqualsEventually(1, unregisterTenantCount);
    }

    private void assertCacheTenantControlCreated(HecatonInstance instance) {
        ICacheService cacheService = getCacheService(instance);
        CacheConfig<?, ?> cacheConfig
                = cacheService.getCacheConfig(CacheUtil.getDistributedObjectName(cacheName));
        TenantControl tc = getTenantControl(instance, cacheConfig);
        assertNotNull("TenantControl should not be null", tc);
        if (hasTenantControl) {
            assertInstanceOf(CountingTenantControl.class, tc);
        } else {
            assertEquals(TenantControl.NOOP_TENANT_CONTROL, tc);
        }
    }
}
