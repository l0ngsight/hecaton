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

package uk.org.hecaton.jet.sql_slow;

import uk.org.hecaton.jet.sql.impl.expression.predicate.ComparisonPredicateIntegrationTest;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ComparisonPredicateIntegrationSlowTest extends ComparisonPredicateIntegrationTest {
    @Parameterized.Parameters(name = "mode:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(CMP_SLOW);
    }

    @Override
    protected Object[] getNumericValues() {
        return NUMERICS_SLOW;
    }
}
