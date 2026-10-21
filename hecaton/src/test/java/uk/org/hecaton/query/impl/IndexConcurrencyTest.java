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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.impl.predicates.SqlPredicate;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.config.IndexType.BITMAP;
import static uk.org.hecaton.config.IndexType.SORTED;
import static uk.org.hecaton.query.Predicates.and;
import static uk.org.hecaton.query.Predicates.between;
import static uk.org.hecaton.query.Predicates.equal;
import static uk.org.hecaton.query.Predicates.greaterEqual;
import static uk.org.hecaton.query.Predicates.greaterThan;
import static uk.org.hecaton.query.Predicates.in;
import static uk.org.hecaton.query.Predicates.lessEqual;
import static uk.org.hecaton.query.Predicates.lessThan;
import static uk.org.hecaton.query.Predicates.like;
import static uk.org.hecaton.query.Predicates.or;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexConcurrencyTest extends AbstractIndexConcurrencyTest {

    @Parameterized.Parameters(name = "indexAttribute: {0}, indexType: {1}")
    public static Collection<Object[]> parameters() {
        // @formatter:off
        return asList(new Object[][]{
                {"age", SORTED},
                {"age", BITMAP},
                {"name", SORTED}
        });
        // @formatter:on
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void testIndexCreationAndQueryDeterministicConcurrency() {
        Config config = getConfig();
        if (indexType == IndexType.BITMAP) {
            // BITMAP indexes need an on-heap map, also when maps default to NATIVE
            config.getMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        }

        HecatonInstance node = createHecatonInstance(config);
        IMap<Integer, Person> map = node.getMap(randomMapName());

        // put some data
        for (int i = 0; i < 10000; ++i) {
            map.put(i, new Person(i));
        }

        // initialize age field access counter
        Person.accessCountDown = new AtomicLong(5000);

        // start indexer, it will await for latch in the middle
        AtomicReference<Throwable> exception = new AtomicReference<>();
        Thread indexer = new Thread(() -> {
            try {
                map.addIndex(indexType, indexAttribute);
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            }
        });
        indexer.start();

        // await for query latch
        assertOpenEventually(Person.queryLatch);
        Person.accessCountDown = null;

        // run checking query
        assertQuery(map, new SqlPredicate("age >= 5000"), 5000);
        assertQuery(map, equal("age", "6000"), 1);
        assertQuery(map, greaterThan("age", "6000"), 3999);
        assertQuery(map, and(greaterThan("age", 6000), lessThan("age", 7000)), 999);
        assertQuery(map, or(equal("age", 6000), equal("age", 7000)), 2);
        assertQuery(map, greaterEqual("age", 5000), 5000);
        assertQuery(map, lessThan("age", 9000), 9000);
        assertQuery(map, lessEqual("age", 9000), 9001);
        assertQuery(map, between("age", 9000, 10000), 1000);
        assertQuery(map, in("age", 5000, 6000, 7000, 11111), 3);
        assertQuery(map, equal("age", "6000"), 1);
        assertQuery(map, like("name", "9999"), 1);

        // open indexer latch
        Person.indexerLatch.countDown();

        // wait for indexer to finish
        assertJoinable(indexer);

        // assert no unexpected exceptions
        assertNull(exception.get());
    }

    private void assertQuery(IMap<Integer, Person> map, Predicate predicate, int expected) {
        Collection<Person> persons = map.values(predicate);
        assertEquals(expected, persons.size());
    }

}
