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

package uk.org.hecaton.config;

import uk.org.hecaton.internal.config.RingbufferStoreConfigReadOnly;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.ringbuffer.RingbufferStore;
import uk.org.hecaton.ringbuffer.RingbufferStoreFactory;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.ringbuffer.impl.RingbufferStoreWrapper;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.test.HecatonTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferStoreConfigTest {

    private RingbufferStoreConfig config = new RingbufferStoreConfig();

    @Test
    public void testDefaultSetting() {
        assertTrue(config.isEnabled());
        assertNull(config.getClassName());
        assertNull(config.getFactoryClassName());
        assertNull(config.getFactoryImplementation());
        assertNull(config.getStoreImplementation());
        assertNotNull(config.getProperties());
        assertTrue(config.getProperties().isEmpty());
    }

    @Test
    public void setStoreImplementation() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        RingbufferStore<Data> store = RingbufferStoreWrapper.create(
                RingbufferService.getRingbufferNamespace("name"),
                config, OBJECT, serializationService, null,
                mock(NodeEngine.class), null);

        config.setStoreImplementation(store);

        assertEquals(store, config.getStoreImplementation());
    }

    @Test
    public void setProperties() {
        Properties properties = new Properties();
        properties.setProperty("key", "value");

        config.setProperties(properties);

        assertEquals(properties, config.getProperties());
    }

    @Test
    public void setProperty() {
        config.setProperty("key", "value");

        assertEquals("value", config.getProperty("key"));
    }

    @Test
    public void setFactoryClassName() {
        config.setFactoryClassName("myFactoryClassName");

        assertEquals("myFactoryClassName", config.getFactoryClassName());
    }

    @Test
    public void setFactoryImplementation() {
        RingbufferStoreFactory factory = new RingbufferStoreFactory() {
            @Override
            public RingbufferStore newRingbufferStore(String name, Properties properties) {
                return null;
            }
        };

        config.setFactoryImplementation(factory);

        assertEquals(factory, config.getFactoryImplementation());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(RingbufferStoreConfig.class)
                      .suppress(Warning.NONFINAL_FIELDS)
                      .withPrefabValues(RingbufferStoreConfigReadOnly.class,
                              new RingbufferStoreConfigReadOnly(new RingbufferStoreConfig().setClassName("red")),
                              new RingbufferStoreConfigReadOnly(new RingbufferStoreConfig().setClassName("black")))
                      .verify();
    }
}
