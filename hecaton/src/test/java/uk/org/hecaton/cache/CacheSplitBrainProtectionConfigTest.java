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

import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.SplitBrainProtectionConfig;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheSplitBrainProtectionConfigTest extends HecatonTestSupport {

    private final URL configUrl = getClass().getClassLoader().getResource("test-hecaton-jcache-with-split-brain-protection.xml");

    @Test
    public void cacheConfigXmlTest() throws IOException {
        final String cacheName = "configtestCache" + randomString();
        Config config = new XmlConfigBuilder(configUrl).build();
        CacheSimpleConfig cacheConfig1 = config.getCacheConfig(cacheName);
        final String splitBrainProtectionName = cacheConfig1.getSplitBrainProtectionName();

        assertEquals("cache-split-brain-protection", splitBrainProtectionName);

        SplitBrainProtectionConfig splitBrainProtectionConfig = config.getSplitBrainProtectionConfig(splitBrainProtectionName);
        assertEquals(3, splitBrainProtectionConfig.getMinimumClusterSize());
        assertEquals(SplitBrainProtectionOn.READ_WRITE, splitBrainProtectionConfig.getProtectOn());
    }
}
