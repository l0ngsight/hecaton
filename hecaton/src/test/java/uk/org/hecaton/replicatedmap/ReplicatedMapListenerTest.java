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

package uk.org.hecaton.replicatedmap;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EntryListenerConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapListenerTest extends AbstractReplicatedMapListenerTest {

    @Test
    public void testRegisterListenerViaConfiguration() {
        String mapName = randomMapName();
        Config config = new Config();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(mapName);
        EntryListenerConfig listenerConfig = new EntryListenerConfig();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        listenerConfig.setImplementation(listener);
        replicatedMapConfig.addEntryListenerConfig(listenerConfig);
        HecatonInstance instance = createHecatonInstance(config);
        ReplicatedMap<Object, Object> replicatedMap = instance.getReplicatedMap(mapName);
        replicatedMap.put(3, 3);

        assertTrueEventually(() -> {
            assertEquals(1, listener.addCount.get());
            assertEquals(Integer.valueOf(3), listener.keys.peek());
        }, 10);
    }

    @Override
    protected <K, V> ReplicatedMap<K, V> createClusterAndGetRandomReplicatedMap() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz = factory.newHecatonInstance();
        factory.newHecatonInstance();
        String mapName = randomMapName();
        return hz.getReplicatedMap(mapName);
    }
}
