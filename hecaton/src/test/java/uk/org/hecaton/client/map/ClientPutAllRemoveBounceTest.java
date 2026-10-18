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

package uk.org.hecaton.client.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.SampleTestObjects.Employee;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.bounce.BounceMemberRule;
import uk.org.hecaton.test.bounce.BounceTestConfiguration;
import uk.org.hecaton.test.jitter.JitterRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

/**
 * Test removing entries by query from Hecaton clients while members are
 * shutting down and joining.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientPutAllRemoveBounceTest extends HecatonTestSupport {
    private static final String TEST_MAP_NAME = "employees";
    private static final int CONCURRENCY = 1;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig()).clusterSize(4).driverCount(4)
            .driverType(BounceTestConfiguration.DriverType.CLIENT).build();

    @Rule
    public JitterRule jitterRule = new JitterRule();

    @Before
    public void setup() {
        bounceMemberRule.getSteadyMember().getMap(TEST_MAP_NAME).addIndex(IndexType.SORTED, "id");
    }

    @Test
    public void testQuery() {
        QueryRunnable[] testTasks = new QueryRunnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            testTasks[i] = new QueryRunnable(bounceMemberRule.getNextTestDriver());
        }
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(1));
    }

    @After
    public void assertMapEmpty() {
        IMap<Integer, Employee> map = bounceMemberRule.getSteadyMember().getMap(TEST_MAP_NAME);
        assertEquals("Map is not empty ", 0, map.size());
    }

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    public static class QueryRunnable implements Runnable {
        private final HecatonInstance hecatonInstance;
        private final Random random = new Random();
        private IMap<Integer, Employee> map;
        int range = 10;
        int keyDomain = Integer.MAX_VALUE;

        QueryRunnable(HecatonInstance hecatonInstance) {
            this.hecatonInstance = hecatonInstance;
        }

        @Override
        public void run() {
            if (map == null) {
                map = hecatonInstance.getMap(TEST_MAP_NAME);
            }
            int min = random.nextInt(keyDomain - range);
            int max = min + range;

            Map<Integer, Employee> m = new HashMap<>();
            for (int i = min; i < max; i++) {
                m.put(i, new Employee(i, "name" + i, i, true, i));
            }

            map.putAll(m);
            map.removeAll(Predicates.sql("id >= " + min + " and id < " + max));
        }
    }
}
