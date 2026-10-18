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

package uk.org.hecaton.client.map.impl.query;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.SampleTestObjects.Value;
import uk.org.hecaton.query.SampleTestObjects.ValueType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static uk.org.hecaton.query.Predicates.equal;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryAdvancedTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    /**
     * Test for issue #5807.
     */
    @Test
    public void queryIndexedComparableField_whenEqualsPredicateWithNullValueIsUsed_thenConverterUsesNullObject() {
        hecatonFactory.newHecatonInstance(getConfig());
        HecatonInstance client = hecatonFactory.newHecatonClient(getClientConfig());
        IMap<Integer, Value> map = client.getMap("default");

        map.addIndex(IndexType.HASH, "type");

        ValueType valueType = new ValueType("name");
        Value valueWithoutNull = new Value("notNull", valueType, 1);
        Value valueWithNull = new Value("null", null, 1);
        map.put(1, valueWithoutNull);
        map.put(2, valueWithNull);

        Predicate nullPredicate = equal("type", null);
        Collection<Value> emptyFieldValues = map.values(nullPredicate);
        assertThat(emptyFieldValues).hasSize(1);
        assertThat(emptyFieldValues).containsExactlyInAnyOrder(valueWithNull);
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
