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

package uk.org.hecaton.client.cache.impl.nearcache;

import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientCacheNearCachePreloaderSlowTest extends ClientCacheNearCachePreloaderTest {

    @Parameters(name = "format:{0} invalidationOnChange:{1} serializeKeys:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, false, true},
                {InMemoryFormat.BINARY, false, false},
                {InMemoryFormat.BINARY, true, true},
                {InMemoryFormat.BINARY, true, false},

                {InMemoryFormat.OBJECT, false, true},
                {InMemoryFormat.OBJECT, false, false},
                {InMemoryFormat.OBJECT, true, true},
                {InMemoryFormat.OBJECT, true, false},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean invalidationOnChange;

    @Parameter(value = 2)
    public boolean serializeKeys;

    @Before
    @Override
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(inMemoryFormat, serializeKeys, invalidationOnChange, KEY_COUNT,
                storeFile.getParent());
    }
}
