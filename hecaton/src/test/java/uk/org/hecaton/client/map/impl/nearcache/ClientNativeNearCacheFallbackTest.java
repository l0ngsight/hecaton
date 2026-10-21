/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.client.map.impl.nearcache;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A client {@code NATIVE} Near Cache without {@code hecaton-hd} (F11) keeps its entries on the heap.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientNativeNearCacheFallbackTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void nativeNearCacheWithoutTheModuleIsOnHeap() {
        HecatonInstance member = factory.newHecatonInstance(smallInstanceConfig());
        ClientConfig config = new ClientConfig();
        config.getNativeMemoryConfig().setEnabled(true);
        config.addNearCacheConfig(new NearCacheConfig("near").setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig().setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE)));
        HecatonInstance client = factory.newHecatonClient(config);

        member.<Integer, String>getMap("near").put(1, "one");
        IMap<Integer, String> map = client.getMap("near");
        assertThat(map.get(1)).isEqualTo("one");
        assertThat(map.get(1)).isEqualTo("one");
        assertThat(map.getLocalMapStats().getNearCacheStats().getHits()).isEqualTo(1);
    }
}
