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

package uk.org.hecaton.cache.impl.journal;

import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.journal.EventJournalReader;
import uk.org.hecaton.journal.AbstractEventJournalBounceTest;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;

/**
 * Cache event journal read bouncing test
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheEventJournalBounceTest extends AbstractEventJournalBounceTest {
    private static final String TEST_CACHE_NAME = "eventJournalTestCache";
    private static final int COUNT_ENTRIES = 100000;

    @Override
    protected void fillDataStructure(HecatonInstance instance) {
        final Cache<String, Integer> cache = instance.getCacheManager().getCache(TEST_CACHE_NAME);

        for (int i = 0; i < COUNT_ENTRIES; i++) {
            cache.put("name" + i, i);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> EventJournalReader<T> getEventJournalReader(HecatonInstance instance) {
        return (EventJournalReader<T>) instance.getCacheManager().getCache(TEST_CACHE_NAME);
    }

    @Override
    protected Config getConfig() {
        final CacheSimpleConfig cacheConfig = new CacheSimpleConfig()
                .setName(TEST_CACHE_NAME);
        cacheConfig.getEvictionConfig().setSize(Integer.MAX_VALUE);
        Config config = super.getConfig()
                             .addCacheConfig(cacheConfig);

        config.getCacheConfig(TEST_CACHE_NAME)
              .setEventJournalConfig(new EventJournalConfig().setEnabled(true)
                                                             .setCapacity(10000));
        return config;
    }
}
