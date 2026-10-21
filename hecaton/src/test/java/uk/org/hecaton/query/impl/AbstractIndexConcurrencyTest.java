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
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.impl.predicates.SqlPredicate;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class AbstractIndexConcurrencyTest extends HecatonTestSupport {

    private static final int QUERY_THREADS_NUM = 5;

    @Rule
    public TestName testName = new TestName();

    @Parameterized.Parameter
    public String indexAttribute;

    @Parameterized.Parameter(1)
    public IndexType indexType;

    @Before
    public void setUp() {
        // by default disable awaiting on latch
        Person.accessCountDown = null;
        Person.indexerLatch = new CountDownLatch(1);
        Person.queryLatch = new CountDownLatch(1);
    }

    @Test
    public void testIndexCreationAndQueryConcurrency() throws InterruptedException {
        Config config = getConfig();
        String mapName = randomMapName();
        if (indexType == IndexType.BITMAP) {
            // BITMAP indexes need an on-heap map, also when maps default to NATIVE
            config.addMapConfig(new MapConfig(mapName).setInMemoryFormat(InMemoryFormat.BINARY));
        }

        HecatonInstance node = createHecatonInstance(config);
        final IMap<Integer, Person> map = node.getMap(mapName);

        // put some data
        for (int i = 0; i < 10000; ++i) {
            map.put(i, new Person(i));
        }

        AtomicReference<Throwable> exception = new AtomicReference<>();

        // run index creation and queries concurrently
        Thread[] threads = new Thread[QUERY_THREADS_NUM + 1];

        threads[0] = new Thread(() -> {
            try {
                map.addIndex(indexType, indexAttribute);
            } catch (Throwable t) {
                exception.compareAndSet(null, t);
            }
        });

        threads[0].start();

        for (int i = 1; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    Collection<Person> persons = map.values(new SqlPredicate("age >= 5000"));
                    assertEquals(5000, persons.size());
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
            threads[i].start();
        }

        // wait for for all threads to finish
        for (int i = 0; i < threads.length; ++i) {
            threads[i].join();
        }

        // assert no unexpected exceptions
        assertNull(exception.get());
    }

    static class Person implements Serializable {

        static AtomicLong accessCountDown;
        static CountDownLatch indexerLatch;
        static CountDownLatch queryLatch;

        public final Integer age;
        public final String name;

        Person(Integer value) {
            this.age = value;
            this.name = value.toString();
        }

        public Integer getAge() {
            updateCounter();
            return age;
        }

        public String getName() {
            updateCounter();
            return name;
        }

        private void updateCounter() {
            if (accessCountDown != null && accessCountDown.decrementAndGet() <= 0) {
                queryLatch.countDown();
                assertOpenEventually(indexerLatch);
            }
        }
    }
}
