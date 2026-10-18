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

import uk.org.hecaton.client.impl.proxy.ClientMapProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.map.AbstractMapIterableTest;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapIterableTest extends AbstractMapIterableTest {
    @Override
    public void setup() {
        factory = new TestHecatonFactory();
        factory.newHecatonInstance(getConfig());
        instance = factory.newHecatonClient();
    }

    @Override
    protected <K, V> Iterable<Map.Entry<K, V>> getIterable(IMap<K, V> map) {
        return ((ClientMapProxy<K, V>) map).iterable(10, prefetchValues);
    }
}
