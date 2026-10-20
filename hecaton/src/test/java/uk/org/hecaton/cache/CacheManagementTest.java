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

import uk.org.hecaton.cache.impl.MXBeanUtil;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheManagementTest extends CacheTestSupport {

    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
    private HecatonInstance hecatonInstance;

    @Override
    protected void onSetup() {
        Config config = createConfig();
        factory.newHecatonInstance(config);
        hecatonInstance = factory.newHecatonInstance(config);
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
        hecatonInstance = null;
    }

    @Override
    protected HecatonInstance getHecatonInstance() {
        return hecatonInstance;
    }

    private void doCacheManagement(boolean enable, boolean stats) {
        String cacheName = randomName();
        CacheConfig cacheConfig = super.createCacheConfig();
        cacheConfig.setManagementEnabled(false);
        cacheConfig.setStatisticsEnabled(false);
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache(cacheName, cacheConfig);
        if (enable) {
            if (stats) {
                cacheManager.enableStatistics(cacheName, true);
            } else {
                cacheManager.enableManagement(cacheName, true);
            }
        }
        if (enable) {
            assertTrue(MXBeanUtil.isRegistered("hecaton", cacheName, stats));
        } else {
            assertFalse(MXBeanUtil.isRegistered("hecaton", cacheName, stats));
        }
    }

    @Test
    public void cacheMXBeanShouldBeRegisteredWhenManagementIsEnabled() {
        doCacheManagement(true, false);
    }

    @Test
    public void cacheMXBeanShouldNotBeRegisteredWhenManagementIsNotEnabled() {
        doCacheManagement(false, false);
    }

    @Test
    public void cacheMXBeanShouldBeRegisteredWhenStatisticsIsEnabled() {
        doCacheManagement(true, true);
    }

    @Test
    public void cacheMXBeanShouldNotBeRegisteredWhenStatisticsIsNotEnabled() {
        doCacheManagement(false, true);
    }

}
