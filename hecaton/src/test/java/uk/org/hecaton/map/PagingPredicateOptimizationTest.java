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

import uk.org.hecaton.query.PagingPredicate;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.query.impl.predicates.InPredicate;
import uk.org.hecaton.query.impl.predicates.PagingPredicateImpl;
import uk.org.hecaton.query.impl.predicates.RuleBasedQueryOptimizer;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PagingPredicateOptimizationTest extends HecatonTestSupport {

    @Test
    public void testInnerPredicateOptimization() {
        RuleBasedQueryOptimizer optimizer = new RuleBasedQueryOptimizer();
        IndexRegistry indexes = mock(IndexRegistry.class);

        Predicate[] orPredicates = new Predicate[10];
        for (int i = 0; i < orPredicates.length; ++i) {
            orPredicates[i] = Predicates.equal("a", i);
        }
        Predicate innerPredicate = Predicates.or(orPredicates);
        PagingPredicate<Object, Object> pagingPredicate = Predicates.pagingPredicate(innerPredicate, 10);

        Predicate optimized = optimizer.optimize(pagingPredicate, indexes);
        assertInstanceOf(PagingPredicateImpl.class, optimized);
        Predicate innerOptimized = ((PagingPredicateImpl) optimized).getPredicate();
        assertInstanceOf(InPredicate.class, innerOptimized);
    }

}
