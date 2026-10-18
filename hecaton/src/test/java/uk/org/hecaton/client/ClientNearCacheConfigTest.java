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

package uk.org.hecaton.client;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientNearCacheConfigTest {

    @Test
    public void testSpecificNearCacheConfig_whenAsteriskAtTheEnd() {
        final ClientConfig clientConfig = new ClientConfig();
        final NearCacheConfig genericNearCacheConfig = new NearCacheConfig();
        genericNearCacheConfig.setName("map*");
        clientConfig.addNearCacheConfig(genericNearCacheConfig);

        final NearCacheConfig specificNearCacheConfig = new NearCacheConfig();
        specificNearCacheConfig.setName("mapStudent*");
        clientConfig.addNearCacheConfig(specificNearCacheConfig);

        final NearCacheConfig mapFoo = clientConfig.getNearCacheConfig("mapFoo");
        final NearCacheConfig mapStudentFoo = clientConfig.getNearCacheConfig("mapStudentFoo");

        assertEquals(genericNearCacheConfig, mapFoo);
        assertEquals(specificNearCacheConfig, mapStudentFoo);
    }

    @Test
    public void testSpecificNearCacheConfig_whenAsteriskAtTheBeginning() {
        final ClientConfig clientConfig = new ClientConfig();
        final NearCacheConfig genericNearCacheConfig = new NearCacheConfig();
        genericNearCacheConfig.setName("*Map");
        clientConfig.addNearCacheConfig(genericNearCacheConfig);

        final NearCacheConfig specificNearCacheConfig = new NearCacheConfig();
        specificNearCacheConfig.setName("*MapStudent");
        clientConfig.addNearCacheConfig(specificNearCacheConfig);

        final NearCacheConfig mapFoo = clientConfig.getNearCacheConfig("fooMap");
        final NearCacheConfig mapStudentFoo = clientConfig.getNearCacheConfig("fooMapStudent");

        assertEquals(genericNearCacheConfig, mapFoo);
        assertEquals(specificNearCacheConfig, mapStudentFoo);
    }

    @Test
    public void testSpecificNearCacheConfig_whenAsteriskInTheMiddle() {
        final ClientConfig clientConfig = new ClientConfig();
        final NearCacheConfig genericNearCacheConfig = new NearCacheConfig();
        genericNearCacheConfig.setName("map*Bar");
        clientConfig.addNearCacheConfig(genericNearCacheConfig);

        final NearCacheConfig specificNearCacheConfig = new NearCacheConfig();
        specificNearCacheConfig.setName("mapStudent*Bar");
        clientConfig.addNearCacheConfig(specificNearCacheConfig);

        final NearCacheConfig mapFoo = clientConfig.getNearCacheConfig("mapFooBar");
        final NearCacheConfig mapStudentFoo = clientConfig.getNearCacheConfig("mapStudentFooBar");

        assertEquals(genericNearCacheConfig, mapFoo);
        assertEquals(specificNearCacheConfig, mapStudentFoo);
    }

}
