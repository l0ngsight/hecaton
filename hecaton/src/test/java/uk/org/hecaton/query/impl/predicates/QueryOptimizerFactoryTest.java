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

import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.spi.properties.HecatonProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.properties.ClusterProperty.QUERY_OPTIMIZER_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOptimizerFactoryTest {

    @Test(expected = IllegalArgumentException.class)
    public void newOptimizer_whenUnknownValue_thenThrowIllegalArgumentException() {
        HecatonProperties hecatonProperties = createMockHecatonProperties(QUERY_OPTIMIZER_TYPE, "foo");
        QueryOptimizerFactory.newOptimizer(hecatonProperties);
    }

    @Test
    public void newOptimizer_whenPropertyContainsRule_thenCreateRulesBasedOptimizer() {
        HecatonProperties hecatonProperties = createMockHecatonProperties(QUERY_OPTIMIZER_TYPE, "RULES");
        QueryOptimizer queryOptimizer = QueryOptimizerFactory.newOptimizer(hecatonProperties);

        assertThat(queryOptimizer).isInstanceOf(RuleBasedQueryOptimizer.class);
    }

    @Test
    public void newOptimizer_whenPropertyContainsNone_thenCreateEmptyOptimizer() {
        HecatonProperties hecatonProperties = createMockHecatonProperties(QUERY_OPTIMIZER_TYPE, "NONE");
        QueryOptimizer queryOptimizer = QueryOptimizerFactory.newOptimizer(hecatonProperties);

        assertThat(queryOptimizer).isInstanceOf(EmptyOptimizer.class);
    }

    private HecatonProperties createMockHecatonProperties(HecatonProperty property, String stringValue) {
        HecatonProperties properties = mock(HecatonProperties.class);
        when(properties.getString(property)).thenReturn(stringValue);
        return properties;
    }
}
