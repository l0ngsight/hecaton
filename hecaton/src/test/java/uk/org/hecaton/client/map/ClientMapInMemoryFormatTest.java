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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapInMemoryFormatTest extends HecatonTestSupport {

    private TestHecatonFactory factory = new TestHecatonFactory();

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testIMapCreation_throwsException_whenInMemoryFormat_NATIVE() {
        Config config = getConfig();
        config.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);

        HecatonInstance member = factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient();
        client.getMap("default");
    }


    @Test(expected = InvalidConfigurationException.class)
    public void testNearCacheCreation_throwsException_whenInMemoryFormat_NATIVE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        Config config = getConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);

        HecatonInstance member = factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient();
        client.getMap("default");
    }
}
