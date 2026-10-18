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

package uk.org.hecaton.core;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.core.Hecaton.getAllHecatonInstances;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class HecatonTest extends HecatonTestSupport {

    public static final String HECATON_CONFIG = "hecaton.config";

    @Before
    @After
    public void cleanup() {
        Hecaton.shutdownAll();
        assertEquals(emptySet(), getAllHecatonInstances());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getOrCreateHecatonInstance_nullConfig() {
        Hecaton.getOrCreateHecatonInstance(null);
    }

    @Test
    public void getOrCreateDefaultHecatonInstance() {
        String hzConfigProperty = System.getProperty(HECATON_CONFIG);
        try {
            System.setProperty(HECATON_CONFIG, "classpath:test-hecaton-jcache.xml");
            HecatonInstance hz1 = Hecaton.getOrCreateHecatonInstance();
            HecatonInstance hz2 = Hecaton.getOrCreateHecatonInstance();
            assertEquals("Calling two times getOrCreateHecatonInstance should return same instance", hz1,
                    hz2);
        } finally {
            if (hzConfigProperty == null) {
                System.clearProperty(HECATON_CONFIG);
            } else {
                System.setProperty(HECATON_CONFIG, hzConfigProperty);
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getOrCreateHecatonInstance_nullName() {
        Config config = new Config();
        Hecaton.getOrCreateHecatonInstance(config);
    }

    @Test
    public void getOrCreateHecatonInstance_noneExisting() {
        Config config = new Config(randomString());
        config.setClusterName(randomString());

        HecatonInstance hz = Hecaton.getOrCreateHecatonInstance(config);

        assertNotNull(hz);
        assertEquals(config.getInstanceName(), hz.getName());
        assertSame(hz, Hecaton.getHecatonInstanceByName(config.getInstanceName()));
        hz.shutdown();
    }

    @Test
    public void getOrCreateHecatonInstance_existing() {
        Config config = new Config(randomString());
        config.setClusterName(randomString());

        HecatonInstance hz1 = Hecaton.newHecatonInstance(config);

        HecatonInstance hz2 = Hecaton.getOrCreateHecatonInstance(config);

        assertSame(hz1, hz2);
        hz1.shutdown();
    }

    @Test
    public void testNewInstanceByName() {
        Config config = new Config();
        config.setInstanceName("test");

        HecatonInstance hc1 = Hecaton.newHecatonInstance(config);
        HecatonInstance hc2 = Hecaton.getHecatonInstanceByName("test");
        HecatonInstance hc3 = Hecaton.getHecatonInstanceByName(hc1.getName());

        assertSame(hc1, hc2);
        assertSame(hc1, hc3);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNewInstanceByNameFail() {
        Config config = new Config();
        config.setInstanceName("test");
        Hecaton.newHecatonInstance(config);
        Hecaton.newHecatonInstance(config);
    }

}
