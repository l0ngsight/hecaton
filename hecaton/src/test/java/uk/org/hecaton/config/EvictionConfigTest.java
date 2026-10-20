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

package uk.org.hecaton.config;

import uk.org.hecaton.spi.eviction.EvictionPolicyComparator;
import uk.org.hecaton.internal.eviction.impl.comparator.LFUEvictionPolicyComparator;
import uk.org.hecaton.internal.eviction.impl.comparator.LRUEvictionPolicyComparator;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static uk.org.hecaton.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static uk.org.hecaton.config.EvictionPolicy.LFU;
import static uk.org.hecaton.config.EvictionPolicy.LRU;
import static uk.org.hecaton.test.HecatonTestSupport.assumeDifferentHashCodes;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EvictionConfigTest {

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(EvictionConfig.class)
                .withIgnoredFields("sizeConfigured")
                .suppress(Warning.NONFINAL_FIELDS)
                .withPrefabValues(EvictionConfig.class,
                        new EvictionConfig().setSize(1000).setMaxSizePolicy(ENTRY_COUNT).setEvictionPolicy(LFU),
                        new EvictionConfig().setSize(300).setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE).setEvictionPolicy(LRU))
                .withPrefabValues(EvictionPolicyComparator.class,
                        new LFUEvictionPolicyComparator(), new LRUEvictionPolicyComparator())
                .verify();
    }

}
