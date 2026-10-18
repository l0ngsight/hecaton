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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.projection.Projection;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class PredicateHzAwareTest extends HecatonTestSupport {

    @Parameters(name = "instanceCount:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1},
                {3},
        });
    }

    @Parameter
    public int instanceCount;

    @Test
    public void testHzAware() {
        final HecatonInstance[] instances = createHecatonInstanceFactory(instanceCount).newInstances();
        final IMap<String, Integer> m = instances[0].getMap("mappy");
        m.put("a", 1);
        m.put("b", 2);

        final Collection<Integer> result = m.project(new SimpleProjection(), new SimplePredicate());

        assertEquals(1, result.size());
        assertEquals(2, (int) result.iterator().next());
    }

    private static class SimpleProjection
            implements Projection<Map.Entry<String, Integer>, Integer>, HecatonInstanceAware, Serializable {

        private transient HecatonInstance instance;

        @Override
        public Integer transform(Map.Entry<String, Integer> input) {
            assertNotNull(instance);
            return input.getValue();
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.instance = hecatonInstance;
        }
    }

    private static class SimplePredicate implements Predicate<String, Integer>, Serializable, HecatonInstanceAware {

        private transient HecatonInstance instance;

        @Override
        public boolean apply(Map.Entry<String, Integer> mapEntry) {
            assertNotNull(instance);
            return mapEntry.getValue() % 2 == 0;
        }

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.instance = hecatonInstance;
        }
    }
}
