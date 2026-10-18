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

import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static uk.org.hecaton.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static uk.org.hecaton.query.impl.TypeConverters.INTEGER_CONVERTER;
import static uk.org.hecaton.query.impl.TypeConverters.LONG_CONVERTER;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConverterResolutionTest {

    private InternalSerializationService serializationService;
    private Extractors extractors;
    private IndexRegistry indexes;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        extractors = Extractors.newBuilder(serializationService).build();
        indexes = IndexRegistry.newBuilder(null, "test", serializationService,
                IndexCopyBehavior.COPY_ON_READ, DEFAULT_IN_MEMORY_FORMAT).extractors(extractors).build();
    }

    @Test
    public void testPopulatedNonCompositeIndex() {
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "value"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.putEntry(new Entry(0, null), null, Index.OperationSource.USER);
        assertNull(indexes.getConverter("value"));
        // just to make sure double-invocation doesn't change anything
        assertNull(indexes.getConverter("value"));

        indexes.putEntry(new Entry(0, 1L), null, Index.OperationSource.USER);
        assertSame(LONG_CONVERTER, indexes.getConverter("value"));

        indexes.destroyIndexes();
        assertNull(indexes.getConverter("value"));
    }

    @Test
    public void testUnpopulatedNonCompositeIndex() {
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.putEntry(new Entry(0, 1L), null, Index.OperationSource.USER);
        assertSame(LONG_CONVERTER, indexes.getConverter("value"));

        indexes.destroyIndexes();
        assertNull(indexes.getConverter("value"));
    }

    @Test
    public void testUnpopulatedCompositeIndex() {
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "__key", "value"));
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.SORTED, "value", "__key"));
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        // just to make sure double-invocation doesn't change anything
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.putEntry(new Entry(0, null), null, Index.OperationSource.USER);
        assertSame(INTEGER_CONVERTER, indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));

        indexes.putEntry(new Entry(0, 1L), null, Index.OperationSource.USER);
        assertSame(INTEGER_CONVERTER, indexes.getConverter("__key"));
        assertSame(LONG_CONVERTER, indexes.getConverter("value"));

        indexes.destroyIndexes();
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
    }

    @Test
    public void testPopulatedCompositeIndex() {
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "__key", "value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.putEntry(new Entry(0, null), null, Index.OperationSource.USER);
        assertSame(INTEGER_CONVERTER, indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));

        indexes.putEntry(new Entry(0, 1L), null, Index.OperationSource.USER);
        assertSame(INTEGER_CONVERTER, indexes.getConverter("__key"));
        assertSame(LONG_CONVERTER, indexes.getConverter("value"));

        indexes.destroyIndexes();
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
    }

    @Test
    public void testCompositeAndNonCompositeIndexes() {
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.SORTED, "value"));
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "__key", "value"));
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.putEntry(new Entry(0, null), null, Index.OperationSource.USER);
        assertSame(INTEGER_CONVERTER, indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.putEntry(new Entry(0, 1L), null, Index.OperationSource.USER);
        assertSame(INTEGER_CONVERTER, indexes.getConverter("__key"));
        assertSame(LONG_CONVERTER, indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));

        indexes.destroyIndexes();
        assertNull(indexes.getConverter("__key"));
        assertNull(indexes.getConverter("value"));
        assertNull(indexes.getConverter("unknown"));
    }

    public static class Value implements Serializable {

        public Long value;

        public Value(Long value) {
            this.value = value;
        }

    }

    private class Entry extends QueryableEntry<Integer, Value> {

        private final int key;
        private final Value value;

        Entry(int key, Long value) {
            this.serializationService = ConverterResolutionTest.this.serializationService;
            this.extractors = ConverterResolutionTest.this.extractors;
            this.key = key;
            this.value = new Value(value);
        }

        @Override
        public Value getValue() {
            return value;
        }

        @Override
        public Integer getKey() {
            return key;
        }

        @Override
        public Data getKeyData() {
            return serializationService.toData(key);
        }

        @Override
        public Data getValueData() {
            return serializationService.toData(value);
        }

        @Override
        protected Object getTargetObject(boolean key) {
            return key ? this.key : value;
        }

        @Override
        public Value setValue(Value value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Integer getKeyIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Data getKeyDataIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Value getValueIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Data getValueDataIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }
    }

}
