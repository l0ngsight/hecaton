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

package uk.org.hecaton.query.impl.predicates;

import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.internal.monitor.impl.MemberPartitionStateImpl;
import uk.org.hecaton.internal.monitor.impl.PerIndexStats;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.query.impl.Index;
import uk.org.hecaton.query.impl.IndexCopyBehavior;
import uk.org.hecaton.query.impl.IndexImpl;
import uk.org.hecaton.query.impl.IndexUtils;
import uk.org.hecaton.query.impl.QueryContext;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import static uk.org.hecaton.query.impl.predicates.PredicateTestUtils.entry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LikePredicateTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testILikePredicateUnicodeCase() {
        assertTrue(new ILikePredicate("this", "Hecaton%").apply(entry("Hecaton is here!")));
        assertTrue(new ILikePredicate("this", "hecaton%").apply(entry("Hecaton is here!")));
        assertTrue(new ILikePredicate("this", "Хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
        assertTrue(new ILikePredicate("this", "хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateUnicodeCase() {
        assertTrue(new LikePredicate("this", "Hecaton%").apply(entry("Hecaton is here!")));
        assertFalse(new LikePredicate("this", "hecaton%").apply(entry("Hecaton is here!")));
        assertTrue(new LikePredicate("this", "Хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
        assertFalse(new LikePredicate("this", "хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateSyntax() {
        assertTrue(new LikePredicate("this", "%Hecaton%").apply(entry("Hecaton is here!")));
        assertTrue(new LikePredicate("this", "%here_").apply(entry("Hecaton is here!")));
        assertTrue(new LikePredicate("this", "%%").apply(entry("Hecaton is here!")));
        assertTrue(new LikePredicate("this", "%%").apply(entry("")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateSyntaxEscape() {
        assertTrue(new LikePredicate("this", "%\\_is\\_%").apply(entry("Hecaton_is_here!")));
        assertTrue(new LikePredicate("this", "%is\\%here!").apply(entry("Hecaton%is%here!")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void negative_testLikePredicateSyntax() {
        assertFalse(new LikePredicate("this", "_Hecaton%").apply(entry("Hecaton is here!")));
        assertFalse(new LikePredicate("this", "_").apply(entry("")));
        assertFalse(new LikePredicate("this", "Hecaton%").apply(entry("")));
    }

    @Test
    public void likePredicateIsNotIndexed_whenBitmapIndexIsUsed() {
        QueryContext queryContext = mock(QueryContext.class);
        Index index = createIndex(IndexType.BITMAP);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(index);

        assertFalse(new LikePredicate("this", "string%").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenHashIndexIsUsed() {
        QueryContext queryContext = mock(QueryContext.class);
        Index index = createIndex(IndexType.HASH);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(index);

        assertFalse(new LikePredicate("this", "string%").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenUnderscoreWildcardIsUsed() {
        QueryContext queryContext = mock(QueryContext.class);
        Index index = createIndex(IndexType.SORTED);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(index);

        assertFalse(new LikePredicate("this", "string_").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenPercentWildcardIsUsedAtTheBeginning() {
        QueryContext queryContext = mock(QueryContext.class);
        Index index = createIndex(IndexType.SORTED);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(index);

        assertFalse(new LikePredicate("this", "%string").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenPercentWildcardIsUsedMultipleTimes() {
        QueryContext queryContext = mock(QueryContext.class);
        Index index = createIndex(IndexType.SORTED);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(index);

        assertFalse(new LikePredicate("this", "sub%string%").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenPercentWildcardIsEscaped() {
        QueryContext queryContext = mock(QueryContext.class);
        Index index = createIndex(IndexType.SORTED);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(index);

        assertFalse(new LikePredicate("this", "sub\\%").isIndexed(queryContext));
        assertFalse(new LikePredicate("this", "sub\\\\\\%").isIndexed(queryContext));
        assertFalse(new LikePredicate("this", "sub\\%string\\%").isIndexed(queryContext));
        assertFalse(new LikePredicate("this", "sub\\str\\%").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenPercentWildcardIsNotTheLastSymbol() {
        QueryContext queryContext = mock(QueryContext.class);
        Index index = createIndex(IndexType.SORTED);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(index);

        assertFalse(new LikePredicate("this", "sub%str").isIndexed(queryContext));
        assertFalse(new LikePredicate("this", "sub%   ").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsIndexed_whenPercentWildcardIsUsed_andIndexIsSorted() {
        QueryContext queryContext = mock(QueryContext.class);
        Index index = createIndex(IndexType.SORTED);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(index);

        assertTrue(new LikePredicate("this", "sub%").isIndexed(queryContext));
        assertTrue(new LikePredicate("this", "sub\\\\%").isIndexed(queryContext));
        assertTrue(new LikePredicate("this", "sub\\%string%").isIndexed(queryContext));
        assertTrue(new LikePredicate("this", "sub\\_string%").isIndexed(queryContext));
    }

    @Nonnull
    private Index createIndex(IndexType indexType) {
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        Extractors mockExtractors = Extractors.newBuilder(mockSerializationService).build();
        IndexConfig config = IndexUtils.createTestIndexConfig(indexType, "this");
        return new IndexImpl(
                null,
                config,
                mockSerializationService,
                mockExtractors,
                IndexCopyBehavior.COPY_ON_READ,
                PerIndexStats.EMPTY,
                MemberPartitionStateImpl.DEFAULT_PARTITION_COUNT,
                "test"
        );
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(LikePredicate.class)
                      .suppress(Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
                      .withRedefinedSuperclass()
                      .verify();
    }

}
