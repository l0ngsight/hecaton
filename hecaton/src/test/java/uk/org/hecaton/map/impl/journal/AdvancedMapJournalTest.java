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

package uk.org.hecaton.map.impl.journal;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.ringbuffer.ReadResultSet;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AdvancedMapJournalTest extends HecatonTestSupport {

    private static final int PARTITION_COUNT = 100;

    private HecatonInstance[] instances;

    @Before
    public void init() {
        instances = createHecatonInstanceFactory().newInstances(getConfig(), 4);
        warmUpPartitions(instances);
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config.getMapConfig("default")
                .setEventJournalConfig(new EventJournalConfig().setEnabled(true));
        return config;
    }

    @Test
    public void testBackupSafety() throws Exception {
        String name = randomMapName();
        IMap<Integer, Object> m = instances[0].getMap(name);
        int keyCount = 1000;
        int updateCount = 3;

        for (int n = 0; n < updateCount; n++) {
            for (int i = 0; i < keyCount; i++) {
                m.set(i, randomString());
            }
        }

        LinkedList<HecatonInstance> instanceList = new LinkedList<>(Arrays.asList(instances));
        waitAllForSafeState(instanceList);

        int expectedSize = keyCount * updateCount;
        while (instanceList.size() > 1) {
            HecatonInstance instance = instanceList.removeFirst();
            instance.getLifecycleService().terminate();
            waitAllForSafeState(instanceList);

            m = instanceList.getFirst().getMap(name);
            int journalSize = getJournalSize(m);
            assertEquals(expectedSize, journalSize);
        }
    }

    private static <K, V> int getJournalSize(IMap<K, V> map) throws ExecutionException, InterruptedException {
        int total = 0;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            CompletionStage<ReadResultSet<Object>> stage =
                    ((MapProxyImpl<K, V>) map).readFromEventJournal(0, 0, 10000, i, null, null);
            ReadResultSet<Object> resultSet = stage.toCompletableFuture().get();
            int readCount = resultSet.readCount();
            total += readCount;
        }
        return total;
    }
}
