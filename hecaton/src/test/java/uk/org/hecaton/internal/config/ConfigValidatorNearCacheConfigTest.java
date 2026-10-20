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

package uk.org.hecaton.internal.config;

import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.InMemoryFormat.NATIVE;
import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static uk.org.hecaton.internal.config.ConfigValidator.checkNearCacheConfig;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigValidatorNearCacheConfigTest extends HecatonTestSupport {

    private static final String MAP_NAME = "default";

    @Test
    public void checkNearCacheConfig_BINARY() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(BINARY), null, false);
    }

    @Test
    public void checkNearCacheConfig_OBJECT() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(OBJECT), null, false);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = InvalidConfigurationException.class)
    public void checkNearCacheConfig_NATIVE() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(NATIVE), null, false);
    }

    /**
     * Not supported client configuration, so test is expected to throw exception.
     */
    @Test(expected = InvalidConfigurationException.class)
    public void checkNearCacheConfig_withUnsupportedClientConfig() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(BINARY), null, true);
    }

    @Test
    public void checkNearCacheConfig_withPreLoaderConfig_onClients() {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(BINARY)
                .setCacheLocalEntries(false);
        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setStoreInitialDelaySeconds(1)
                .setStoreInitialDelaySeconds(1);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, null, true);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkNearCacheConfig_withPreloaderConfig_onMembers() {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(BINARY);
        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setStoreInitialDelaySeconds(1)
                .setStoreInitialDelaySeconds(1);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, null, false);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkNearCacheConfig_withLocalUpdatePolicy_CACHE_ON_UPDATE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setLocalUpdatePolicy(CACHE_ON_UPDATE);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, null, false);
    }

    @Test
    public void checkNearCacheConfig_withLocalUpdatePolicy_INVALIDATE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setLocalUpdatePolicy(INVALIDATE);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, null, false);
    }

    private NearCacheConfig getNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setInMemoryFormat(inMemoryFormat)
                .setCacheLocalEntries(true);
    }
}
