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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.backup.BackupAccessor;
import uk.org.hecaton.test.backup.TestBackupUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static uk.org.hecaton.test.backup.TestBackupUtils.assertExpiryPolicyEventually;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HecatonSerialClassRunner.class)
public class CacheExpiryPolicyBackupTest extends HecatonTestSupport {

    private static final int NINSTANCES = 3;
    private static final int ENTRIES = 100;
    private TestHecatonInstanceFactory factory;
    private HecatonInstance[] instances;
    private String cacheName;

    @Before
    public void setUp() {
        cacheName = randomName();
        factory = createHecatonInstanceFactory(NINSTANCES);
        instances = new HecatonInstance[NINSTANCES];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHecatonInstance(getConfig());
        }
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        return new Config().addCacheConfig(getCacheConfig());
    }

    protected CacheSimpleConfig getCacheConfig() {
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setBackupCount(NINSTANCES - 1);
        return cacheConfig;
    }

    private void assertExpiryPolicy(final ExpiryPolicy expiryPolicy, final Collection<String> keys) {
        for (int replicaIndex = 1; replicaIndex < NINSTANCES; replicaIndex++) {
            final BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cacheName, replicaIndex);

            for (String key: keys) {
                assertExpiryPolicyEventually(key, expiryPolicy, backupAccessor);
            }
        }
    }

    @Test
    public void testSetExpiryPolicyBackupOperation() {
        HecatonInstance instance = instances[0];
        ICache<String, String> cache = instance.getCacheManager().getCache(cacheName);
        Set<String> keys = new HashSet<>();

        for (int i = 0; i < ENTRIES; i++) {
            cache.put("key" + i, "value");
            keys.add("key" + i);
        }

        ExpiryPolicy expiryPolicy = new EternalExpiryPolicy();
        cache.setExpiryPolicy(keys, expiryPolicy);

        assertExpiryPolicy(expiryPolicy, keys);
    }
}
