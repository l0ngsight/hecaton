/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.sql.impl.opt.physical.index;

import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonIntegerType;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexCompositeFilter;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexEqualsFilter;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexFilterValue;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexRangeFilter;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexComponentFilterResolverTest {
    private static final RexInputRef REX_INPUT_REF = new RexInputRef(0, HecatonIntegerType.create(SqlTypeName.INTEGER, false));
    private static final QueryDataType QUERY_DATA_TYPE = QueryDataType.INT;

    private static final IndexComponentCandidate SINGLE_EQUALITY_CANDIDATE = new IndexComponentCandidate(REX_INPUT_REF, 0,
            new IndexEqualsFilter());
    private static final IndexComponentCandidate TWO_EQUALITIES_CANDIDATE = new IndexComponentCandidate(REX_INPUT_REF, 0,
            new IndexCompositeFilter(new IndexEqualsFilter(), new IndexEqualsFilter()));
    private static final IndexComponentCandidate LOWER_BOUND_RANGE_CANDIDATE = new IndexComponentCandidate(REX_INPUT_REF, 0,
            new IndexRangeFilter(new IndexFilterValue(), true, null, false));
    private static final IndexComponentCandidate UPPER_BOUND_RANGE_CANDIDATE = new IndexComponentCandidate(REX_INPUT_REF, 0,
            new IndexRangeFilter(null, false, new IndexFilterValue(), true));
    private static final IndexComponentCandidate BOTH_BOUNDS_RANGE_CANDIDATE = new IndexComponentCandidate(REX_INPUT_REF, 0,
            new IndexRangeFilter(new IndexFilterValue(), true, new IndexFilterValue(), true));
    private static final IndexComponentCandidate EQUALITY_AND_RANGE_CANDIDATE = new IndexComponentCandidate(REX_INPUT_REF, 0,
            new IndexCompositeFilter(new IndexRangeFilter(), new IndexEqualsFilter()));

    private static final List<IndexComponentCandidate> WITH_SINGLE_EQUALITY_CANDIDATES = Arrays.asList(
            EQUALITY_AND_RANGE_CANDIDATE, LOWER_BOUND_RANGE_CANDIDATE, TWO_EQUALITIES_CANDIDATE, SINGLE_EQUALITY_CANDIDATE);
    private static final List<IndexComponentCandidate> WITH_TWO_EQUALITIES_AS_BEST_CANDIDATES = Arrays.asList(
            EQUALITY_AND_RANGE_CANDIDATE, LOWER_BOUND_RANGE_CANDIDATE, TWO_EQUALITIES_CANDIDATE);
    private static final List<IndexComponentCandidate> WITH_LOWER_BOUND_RANGE_AS_BEST_CANDIDATES = Arrays.asList(
            EQUALITY_AND_RANGE_CANDIDATE, LOWER_BOUND_RANGE_CANDIDATE);
    private static final List<IndexComponentCandidate> WITH_UPPER_BOUND_RANGE_AS_BEST_CANDIDATES = Arrays.asList(
            EQUALITY_AND_RANGE_CANDIDATE, UPPER_BOUND_RANGE_CANDIDATE);
    private static final List<IndexComponentCandidate> WITH_TWO_RANGES_AS_BEST_CANDIDATES = Arrays.asList(
            EQUALITY_AND_RANGE_CANDIDATE, UPPER_BOUND_RANGE_CANDIDATE, LOWER_BOUND_RANGE_CANDIDATE);
    private static final List<IndexComponentCandidate> WITH_BOTH_BOUNDS_AS_BEST_CANDIDATES = Arrays.asList(
            EQUALITY_AND_RANGE_CANDIDATE, BOTH_BOUNDS_RANGE_CANDIDATE);
    private static final List<IndexComponentCandidate> WITH_EQUALITY_AND_RANGE_FILTER_AS_BEST_CANDIDATES = Arrays.asList(
            EQUALITY_AND_RANGE_CANDIDATE);

    @Parameterized.Parameter
    public IndexType indexType;

    @Test
    public void when_singleEqualityFilterPresent_then_itIsUsed() {
        IndexComponentFilter bestFilter = IndexComponentFilterResolver.findBestComponentFilter(
                indexType, WITH_SINGLE_EQUALITY_CANDIDATES, QUERY_DATA_TYPE
        );

        assertEquals(SINGLE_EQUALITY_CANDIDATE.getFilter(), bestFilter.getFilter());
    }

    @Test
    public void when_twoEqualitiesFilterPresentAndNoBetterChoice_then_itIsUsed() {
        IndexComponentFilter bestFilter = IndexComponentFilterResolver.findBestComponentFilter(
                indexType, WITH_TWO_EQUALITIES_AS_BEST_CANDIDATES, QUERY_DATA_TYPE
        );

        assertEquals(TWO_EQUALITIES_CANDIDATE.getFilter(), bestFilter.getFilter());
    }

    @Test
    public void when_lowerBoundRangeFilterPresentAndNoBetterChoiceAndSortedIndex_then_itIsUsed() {
        IndexComponentFilter bestFilter = IndexComponentFilterResolver.findBestComponentFilter(
                indexType, WITH_LOWER_BOUND_RANGE_AS_BEST_CANDIDATES, QUERY_DATA_TYPE
        );

        if (indexType == IndexType.SORTED) {
            assertEquals(bestFilter.getFilter(), LOWER_BOUND_RANGE_CANDIDATE.getFilter());
        } else {
            assertNull(bestFilter);
        }
    }

    @Test
    public void when_upperBoundRangeFilterPresentAndNoBetterChoiceAndSortedIndex_then_itIsUsed() {
        IndexComponentFilter bestFilter = IndexComponentFilterResolver.findBestComponentFilter(
                indexType, WITH_UPPER_BOUND_RANGE_AS_BEST_CANDIDATES, QUERY_DATA_TYPE
        );

        if (indexType == IndexType.SORTED) {
            assertEquals(bestFilter.getFilter(), UPPER_BOUND_RANGE_CANDIDATE.getFilter());
        } else {
            assertNull(bestFilter);
        }
    }

    @Test
    public void when_bothBoundsRangeFilterPresentAndNoBetterChoiceAndSortedIndex_then_itIsUsed() {
        IndexComponentFilter bestFilter = IndexComponentFilterResolver.findBestComponentFilter(
                indexType, WITH_BOTH_BOUNDS_AS_BEST_CANDIDATES, QUERY_DATA_TYPE
        );

        if (indexType == IndexType.SORTED) {
            assertEquals(bestFilter.getFilter(), BOTH_BOUNDS_RANGE_CANDIDATE.getFilter());
        } else {
            assertNull(bestFilter);
        }
    }

    @Test
    public void when_equalityAndRangeFilterPresentNoBetterChoiceAndSortedIndex_then_itIsUsed() {
        IndexComponentFilter bestFilter = IndexComponentFilterResolver.findBestComponentFilter(
                indexType, WITH_EQUALITY_AND_RANGE_FILTER_AS_BEST_CANDIDATES, QUERY_DATA_TYPE
        );

        if (indexType == IndexType.SORTED) {
            assertEquals(bestFilter.getFilter(), EQUALITY_AND_RANGE_CANDIDATE.getFilter());
        } else {
            assertNull(bestFilter);
        }
    }

    @Test
    public void when_twoRangeFilterPresentAndNoBetterChoiceAndSortedIndex_then_filtersAreCombined() {
        IndexComponentFilter bestFilter = IndexComponentFilterResolver.findBestComponentFilter(
                indexType, WITH_TWO_RANGES_AS_BEST_CANDIDATES, QUERY_DATA_TYPE
        );

        if (indexType == IndexType.SORTED) {
            assertTrue(bestFilter.getFilter() instanceof IndexRangeFilter);
            IndexRangeFilter resultFilter = (IndexRangeFilter) bestFilter.getFilter();
            IndexRangeFilter lowerBound = (IndexRangeFilter) LOWER_BOUND_RANGE_CANDIDATE.getFilter();
            IndexRangeFilter upperBound = (IndexRangeFilter) UPPER_BOUND_RANGE_CANDIDATE.getFilter();
            assertEquals(resultFilter.getFrom(), lowerBound.getFrom());
            assertEquals(resultFilter.isFromInclusive(), lowerBound.isFromInclusive());
            assertEquals(resultFilter.getTo(), upperBound.getTo());
            assertEquals(resultFilter.isToInclusive(), upperBound.isToInclusive());
        } else {
            assertNull(bestFilter);
        }
    }

    @Parameterized.Parameters(name = "indexType:{0}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();
        for (IndexType indexType : Arrays.asList(IndexType.SORTED, IndexType.HASH)) {
            res.add(new Object[]{indexType});
        }

        return res;
    }
}
