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

package uk.org.hecaton.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BackupEntryProcessorInstanceAwareTest extends HecatonTestSupport {
    public static final String MAP_NAME = "EntryProcessorTest";

    @Override
    public Config getConfig() {
        final MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setReadBackupData(true)
                .setInMemoryFormat(BINARY);
        return super.getConfig().addMapConfig(mapConfig);
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        final Config cfg = getConfig();
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance i1 = factory.newHecatonInstance(cfg);
        final HecatonInstance i2 = factory.newHecatonInstance(cfg);
        final IMap<String, Integer> m1 = i1.getMap(MAP_NAME);
        final IMap<String, Integer> m2 = i2.getMap(MAP_NAME);

        m1.put("a", 1);
        m1.put("b", 2);
        m1.executeOnEntries(new PartitionAwareTestEntryProcessor());

        for (final String key : m1.keySet()) {
            assertTrueEventually(() -> {
                final Integer k = m1.get(key);
                final Integer v = m2.get(key);
                assertEquals(k, v);
            });
        }
        i1.shutdown();
    }

    private static class PartitionAwareTestEntryProcessor
            implements EntryProcessor<String, Integer, Object>, HecatonInstanceAware {

        private transient HecatonInstance hz;

        @Override
        public Object process(Map.Entry<String, Integer> entry) {
            if (hz != null) {
                entry.setValue(entry.getValue() + 1);
            }
            return null;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.hz = hecatonInstance;
        }
    }
}
