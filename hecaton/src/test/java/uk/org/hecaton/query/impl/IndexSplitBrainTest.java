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

package uk.org.hecaton.query.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.SplitBrainTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexSplitBrainTest extends SplitBrainTestSupport {

    private final String mapName = randomMapName();

    private String key;
    private ValueObject value;

    @Override
    protected int[] brains() {
        return new int[]{1, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HecatonInstance[] instances) {
        warmUpPartitions(instances);
        key = generateKeyOwnedBy(instances[0]);
        value = new ValueObject(key);

        final IMap<String, ValueObject> map1 = instances[0].getMap(mapName);
        final IMap<String, ValueObject> map2 = instances[1].getMap(mapName);

        map1.put(key, value);
        assertNotNull("Entry should exist in map2 before split", map2.get(key));
    }

    @Override
    protected void onAfterSplitBrainCreated(HecatonInstance[] firstBrain, HecatonInstance[] secondBrain) {
        final IMap<String, ValueObject> map1 = firstBrain[0].getMap(mapName);
        final IMap<String, ValueObject> map2 = secondBrain[0].getMap(mapName);
        map1.remove(key);
        assertNotNull("Entry should exist in map2 during split", map2.get(key));
    }

    @Override
    protected void onAfterSplitBrainHealed(HecatonInstance[] instances) {
        final IMap<String, ValueObject> map1 = instances[0].getMap(mapName);
        final IMap<String, ValueObject> map2 = instances[1].getMap(mapName);

        assertTrueEventually(() -> assertNotNull("Entry should exist in map1 after merge", map1.get(key)), 15);
        map1.remove(key);
        assertTrueAllTheTime(() -> {
            Predicate predicate = Predicates.equal("id", value.getId());
            Collection<ValueObject> values = map1.values(predicate);
            assertThat(values).isEmpty();

            values = map2.values(predicate);
            assertThat(values).isEmpty();
        }, 5);
    }

    @Override
    protected Config config() {
        Config config = super.config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "id"));
        return config;
    }

    private static class ValueObject implements DataSerializable {

        private String id;

        ValueObject() {
        }

        ValueObject(String id) {
            this.id = id;
        }

        public String getId() {
            return this.id;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(this.id);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.id = in.readString();
        }
    }
}
