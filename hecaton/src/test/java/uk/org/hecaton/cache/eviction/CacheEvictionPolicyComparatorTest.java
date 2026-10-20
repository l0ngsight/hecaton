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

package uk.org.hecaton.cache.eviction;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.cache.impl.maxsize.impl.EntryCountCacheEvictionChecker.calculateMaxPartitionSize;
import static uk.org.hecaton.config.EvictionConfig.DEFAULT_MAX_ENTRY_COUNT;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheEvictionPolicyComparatorTest extends AbstractCacheEvictionPolicyComparatorTest {

    @Override
    protected CachingProvider createCachingProvider(HecatonInstance instance) {
        return createServerCachingProvider(instance);
    }

    @Override
    protected HecatonInstance createInstance(Config config) {
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory();
        return instanceFactory.newHecatonInstance(config);
    }

    @Override
    protected ConcurrentMap<String, Object> getUserContext(HecatonInstance hecatonInstance) {
        return hecatonInstance.getUserContext();
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorClassName_when_maxSizePolicy_is_entryCount() {
        int partitionCount = Integer.parseInt(ClusterProperty.PARTITION_COUNT.getDefaultValue());
        int iterationCount = (calculateMaxPartitionSize(DEFAULT_MAX_ENTRY_COUNT, partitionCount) * partitionCount) * 2;
        EvictionConfig evictionConfig = new EvictionConfig().setComparatorClassName(MyEvictionPolicyComparator.class.getName());

        testEvictionPolicyComparator(evictionConfig, iterationCount);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorInstance_when_maxSizePolicy_is_entryCount() {
        int partitionCount = Integer.parseInt(ClusterProperty.PARTITION_COUNT.getDefaultValue());
        int iterationCount = (calculateMaxPartitionSize(DEFAULT_MAX_ENTRY_COUNT, partitionCount) * partitionCount) * 2;
        EvictionConfig evictionConfig = new EvictionConfig().setComparator(new MyEvictionPolicyComparator());

        testEvictionPolicyComparator(evictionConfig, iterationCount);
    }
}
