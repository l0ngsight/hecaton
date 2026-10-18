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

package uk.org.hecaton.map.metrics;

import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.query.impl.IndexCopyBehavior;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.query.impl.IndexUtils;
import uk.org.hecaton.query.impl.InternalIndex;
import uk.org.hecaton.query.impl.QueryContext.IndexMatchHint;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import static uk.org.hecaton.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

@ParallelJVMTest
@QuickTest
class IndexNotReadyCountTest {

    @Test
    void testCounterIncrementOnUnindexedPartitions() {
        IndexRegistry indexRegistry = IndexRegistry.newBuilder(null, "test", new DefaultSerializationServiceBuilder().build(),
                IndexCopyBehavior.NEVER, DEFAULT_IN_MEMORY_FORMAT).global(true).statsEnabled(true).build();

        InternalIndex index = indexRegistry.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "abc"));

        assertThat(indexRegistry.matchIndex("abc", IndexMatchHint.NONE, 0)).isSameAs(index);
        assertThat(index.getPerIndexStats().getIndexNotReadyQueryCount()).isZero();

        assertThat(indexRegistry.matchIndex("abc", IndexMatchHint.NONE, 1)).isNull();
        assertThat(index.getPerIndexStats().getIndexNotReadyQueryCount()).isEqualTo(1);
    }
}
