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

import uk.org.hecaton.config.CacheDeserializedValues;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MetadataPolicy;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.query.LocalIndexStats;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexDeserializationTest extends HecatonTestSupport {

    private static final int ENTRY_COUNT = 10;

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public CacheDeserializedValues cacheDeserializedValues;

    @Parameters(name = "inMemoryFormat: {0}, cacheDeserializedValues: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.OBJECT, CacheDeserializedValues.NEVER},
                {InMemoryFormat.BINARY, CacheDeserializedValues.NEVER},
                {InMemoryFormat.OBJECT, CacheDeserializedValues.INDEX_ONLY},
                {InMemoryFormat.BINARY, CacheDeserializedValues.INDEX_ONLY},
                {InMemoryFormat.OBJECT, CacheDeserializedValues.ALWAYS},
                {InMemoryFormat.BINARY, CacheDeserializedValues.ALWAYS}
        });
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getMapConfig("map")
                .setInMemoryFormat(inMemoryFormat)
                .setCacheDeserializedValues(cacheDeserializedValues)
                .setMetadataPolicy(MetadataPolicy.OFF)
                .addIndexConfig(new IndexConfig(IndexType.HASH, "v1"))
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "v2"))
                .addIndexConfig(new IndexConfig(IndexType.BITMAP, "v3"));
        return config;
    }

    @Test
    public void testDeserialization() {
        IMap<Integer, Record> map = createHecatonInstance().getMap("map");

        Record.deserializationCount.set(0);
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            map.put(i, new Record(i));
        }

        assertEquals(ENTRY_COUNT, Record.deserializationCount.get());
        assertEquals(3, map.getLocalMapStats().getIndexStats().size());
        for (LocalIndexStats indexStats : map.getLocalMapStats().getIndexStats().values()) {
            assertEquals(ENTRY_COUNT, indexStats.getInsertCount());
        }

        Record.deserializationCount.set(0);
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            map.set(i, new Record(i));
        }
        assertEquals(inMemoryFormat == InMemoryFormat.OBJECT ? ENTRY_COUNT : ENTRY_COUNT * 2, Record.deserializationCount.get());
        assertEquals(3, map.getLocalMapStats().getIndexStats().size());
        for (LocalIndexStats indexStats : map.getLocalMapStats().getIndexStats().values()) {
            assertEquals(ENTRY_COUNT, indexStats.getUpdateCount());
        }

        Record.deserializationCount.set(0);
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            map.delete(i);
        }
        assertEquals(inMemoryFormat == InMemoryFormat.OBJECT
                || cacheDeserializedValues != CacheDeserializedValues.NEVER ? 0 : ENTRY_COUNT, Record.deserializationCount.get());
        assertEquals(3, map.getLocalMapStats().getIndexStats().size());
        for (LocalIndexStats indexStats : map.getLocalMapStats().getIndexStats().values()) {
            assertEquals(ENTRY_COUNT, indexStats.getRemoveCount());
        }
    }

    static class Record implements DataSerializable {

        static final AtomicInteger deserializationCount = new AtomicInteger();

        int v1;
        int v2;
        int v3;

        @SuppressWarnings("unused")
        Record() {
        }

        Record(int key) {
            this.v1 = key * 1000;
            this.v2 = key * 1001;
            this.v3 = key * 1002;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(v1);
            out.writeInt(v2);
            out.writeInt(v3);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            v1 = in.readInt();
            v2 = in.readInt();
            v3 = in.readInt();

            deserializationCount.incrementAndGet();
        }

    }

}
