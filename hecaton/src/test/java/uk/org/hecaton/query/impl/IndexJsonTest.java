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
import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.query.impl.predicates.AndPredicate;
import uk.org.hecaton.query.impl.predicates.EqualPredicate;
import uk.org.hecaton.query.impl.predicates.SqlPredicate;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static uk.org.hecaton.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static uk.org.hecaton.internal.util.IterableUtil.size;
import static uk.org.hecaton.query.impl.IndexRegistry.SKIP_PARTITIONS_COUNT_CHECK;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class IndexJsonTest {

    @Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {IndexCopyBehavior.COPY_ON_READ},
                {IndexCopyBehavior.COPY_ON_WRITE},
                {IndexCopyBehavior.NEVER},
        });
    }

    final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testJsonIndex() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        IndexRegistry indexes = IndexRegistry.newBuilder(null, "test", ss, copyBehavior, DEFAULT_IN_MEMORY_FORMAT).extractors(Extractors.newBuilder(ss).build()).indexProvider(
                new DefaultIndexProvider()).usesCachedQueryableEntries(true).statsEnabled(true).global(true).build();
        Index numberIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "age"));
        Index boolIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "active"));
        Index stringIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "name"));

        for (int i = 0; i < 1001; i++) {
            Data key = ss.toData(i);
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            indexes.putEntry(new QueryEntry(ss, key, new HecatonJsonValue(jsonString), Extractors.newBuilder(ss).build()), null, Index.OperationSource.USER);
        }

        assertEquals(1, numberIndex.getRecords(10).size());
        assertEquals(0, numberIndex.getRecords(-1).size());
        assertEquals(1001, stringIndex.getRecords("sancar").size());
        assertEquals(501, boolIndex.getRecords(true).size());
        assertEquals(501, size(indexes.query(new AndPredicate(new EqualPredicate("name", "sancar"), new EqualPredicate("active", "true")), SKIP_PARTITIONS_COUNT_CHECK)));
        assertEquals(300, size(indexes.query(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true)), SKIP_PARTITIONS_COUNT_CHECK)));
        assertEquals(1001, size(indexes.query(new SqlPredicate("name == sancar"), SKIP_PARTITIONS_COUNT_CHECK)));
    }
}
