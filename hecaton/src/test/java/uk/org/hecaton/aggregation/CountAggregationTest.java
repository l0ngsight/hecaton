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

package uk.org.hecaton.aggregation;

import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static uk.org.hecaton.aggregation.TestSamples.createEntryWithValue;
import static uk.org.hecaton.aggregation.TestSamples.createExtractableEntryWithValue;
import static uk.org.hecaton.aggregation.TestSamples.sampleBigDecimals;
import static uk.org.hecaton.aggregation.TestSamples.samplePersons;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CountAggregationTest {

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator() {
        List<BigDecimal> values = sampleBigDecimals();
        long expectation = values.size();

        Aggregator<Entry<BigDecimal, BigDecimal>, Long> aggregation = Aggregators.count();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Entry<BigDecimal, BigDecimal>, Long> resultAggregation = Aggregators.count();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result).isEqualTo(expectation);
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator_withAttributePath() {
        List<Person> values = samplePersons();
        long expectation = values.size();

        Aggregator<Map.Entry<Person, Person>, Long> aggregation = Aggregators.count("age");
        for (Person person : values) {
            aggregation.accumulate(createExtractableEntryWithValue(person, ss));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> resultAggregation = Aggregators.count("age");
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result).isEqualTo(expectation);
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator_withNull() {
        List<BigDecimal> values = sampleBigDecimals();
        values.add(null);
        long expectation = values.size();

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> aggregation = Aggregators.count();
        for (BigDecimal value : values) {
            aggregation.accumulate(createEntryWithValue(value));
        }

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, Long> resultAggregation = Aggregators.count();
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result).isEqualTo(expectation);
    }

    @Test(timeout = TimeoutInMillis.MINUTE)
    public void testCountAggregator_withAttributePath_withNull() {
        List<Person> values = samplePersons();
        values.add(null);
        long expectation = values.size();

        Aggregator<Entry<Person, Person>, Long> aggregation = Aggregators.count("age");
        for (Person person : values) {
            aggregation.accumulate(createExtractableEntryWithValue(person, ss));
        }

        Aggregator<Entry<BigDecimal, BigDecimal>, Long> resultAggregation = Aggregators.count("age");
        resultAggregation.combine(aggregation);
        long result = resultAggregation.aggregate();

        assertThat(result).isEqualTo(expectation);
    }
}
