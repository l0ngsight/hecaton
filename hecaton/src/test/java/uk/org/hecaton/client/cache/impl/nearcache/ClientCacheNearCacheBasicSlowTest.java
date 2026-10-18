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
import uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy;
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

import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static java.util.Arrays.asList;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheBasicSlowTest extends ClientCacheNearCacheBasicTest {

    @Parameters(name = "format:{0} serializeKeys:{1} localUpdatePolicy:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.BINARY, true, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.BINARY, false, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.BINARY, false, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.OBJECT, true, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.OBJECT, true, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.OBJECT, false, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.OBJECT, false, LocalUpdatePolicy.CACHE_ON_UPDATE},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean serializeKeys;

    @Parameter(value = 2)
    public LocalUpdatePolicy localUpdatePolicy;

    @Before
    @Override
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(inMemoryFormat, serializeKeys)
                .setLocalUpdatePolicy(localUpdatePolicy);
    }
}
