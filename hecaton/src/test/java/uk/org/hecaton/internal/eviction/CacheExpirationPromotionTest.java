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

package uk.org.hecaton.internal.eviction;

import uk.org.hecaton.cache.HecatonCacheManager;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.impl.HecatonServerCacheManager;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.backup.BackupAccessor;
import uk.org.hecaton.test.backup.TestBackupUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static uk.org.hecaton.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class CacheExpirationPromotionTest extends HecatonTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS, "1");

    private String cacheName = "test";
    private int nodeCount = 3;
    private TestHecatonInstanceFactory factory;
    private HecatonInstance[] instances;

    protected CacheConfig getCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setBackupCount(1);
        return cacheConfig;
    }

    @Before
    public void setup() {
        factory = createHecatonInstanceFactory(nodeCount);
        instances = new HecatonInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHecatonInstance(getConfig());
        }
    }

    @Test
    public void promoted_replica_should_send_eviction_to_other_backup() {
        final CachingProvider provider = createServerCachingProvider(instances[0]);
        provider.getCacheManager().createCache(cacheName, getCacheConfig());
        HecatonCacheManager cacheManager = (HecatonServerCacheManager) provider.getCacheManager();

        final String keyOwnedByLastInstance = generateKeyOwnedBy(instances[nodeCount - 1]);

        ICache<String, String> cache = cacheManager.getCache(cacheName).unwrap(ICache.class);
        cache.put(keyOwnedByLastInstance, "dummyVal", new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 5)));

        final BackupAccessor<String, String> backupAccessor = TestBackupUtils.newCacheAccessor(instances, cacheName, 1);

        assertTrueEventually(() -> assertNotNull(backupAccessor.get(keyOwnedByLastInstance)));

        instances[nodeCount - 1].shutdown();

        // the backup replica became the primary, now the backup is the other node.
        // we check if the newly appointed replica sent expiration to backups
        assertTrueEventually(() -> assertEquals(0, backupAccessor.size()));
    }
}
