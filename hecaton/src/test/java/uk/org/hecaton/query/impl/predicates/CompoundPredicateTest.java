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

import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static uk.org.hecaton.test.ReflectionsHelper.REFLECTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test all classes which implement CompoundStatement in package uk.org.hecaton.query.impl.predicates
 * for compliance with CompoundStatement contract.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompoundPredicateTest {

    @Parameter
    public Class<? extends CompoundPredicate> klass;

    @Parameters(name = "{0}")
    public static Collection<Class<? extends CompoundPredicate>> getCompoundPredicateImplementations() {
        // locate all classes which implement CompoundPredicate and exercise them
        return REFLECTIONS.getSubTypesOf(CompoundPredicate.class);
    }

    @Test
    public void test_newInstance()
            throws IllegalAccessException, InstantiationException {
        // all CompoundStatement classes must provide a default constructor
        Object o = klass.newInstance();
        assertTrue(o instanceof CompoundPredicate);
    }

    @Test
    public void test_whenSetPredicatesOnNewInstance()
            throws IllegalAccessException, InstantiationException {

        CompoundPredicate o = klass.newInstance();
        Predicate truePredicate = Predicates.alwaysTrue();
        o.setPredicates(new Predicate[]{truePredicate});
        assertEquals(truePredicate, o.getPredicates()[0]);
    }

    @Test(expected = IllegalStateException.class)
    public void test_whenSetPredicatesOnExistingPredicates_thenThrowException()
            throws IllegalAccessException, InstantiationException {

        CompoundPredicate o = klass.newInstance();
        Predicate truePredicate = Predicates.alwaysTrue();
        o.setPredicates(new Predicate[]{truePredicate});

        Predicate falsePredicate = Predicates.alwaysFalse();
        o.setPredicates(new Predicate[]{falsePredicate});

        fail();
    }

}
