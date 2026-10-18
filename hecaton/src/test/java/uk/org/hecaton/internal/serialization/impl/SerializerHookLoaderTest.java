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

package uk.org.hecaton.internal.serialization.impl;

import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.config.SerializerConfig;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * SerializerHookLoader Tester.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SerializerHookLoaderTest extends HecatonTestSupport {

    private ClassLoader classLoader = getClass().getClassLoader();

    @Test
    public void testLoad_withDefaultConstructor() {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setClassName("uk.org.hecaton.internal.serialization.impl.TestSerializerHook$TestSerializer");
        serializerConfig.setTypeClassName("uk.org.hecaton.internal.serialization.impl.SampleIdentifiedDataSerializable");

        SerializationConfig serializationConfig = getConfig().getSerializationConfig();
        serializationConfig.addSerializerConfig(serializerConfig);

        SerializerHookLoader hook = new SerializerHookLoader(serializationConfig, classLoader);
        Map<Class, Object> serializers = hook.getSerializers();
        assertNotNull(serializers);
    }

    @Test
    public void testLoad_withParametrizedConstructor() {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setClassName("uk.org.hecaton.internal.serialization.impl.TestSerializerHook$TestSerializerWithTypeConstructor");
        serializerConfig.setTypeClassName("uk.org.hecaton.internal.serialization.impl.SampleIdentifiedDataSerializable");

        SerializationConfig serializationConfig = getConfig().getSerializationConfig();
        serializationConfig.addSerializerConfig(serializerConfig);

        SerializerHookLoader hook = new SerializerHookLoader(serializationConfig, classLoader);
        Map<Class, Object> serializers = hook.getSerializers();

        TestSerializerHook.TestSerializerWithTypeConstructor serializer = (TestSerializerHook.TestSerializerWithTypeConstructor)
                serializers.get(SampleIdentifiedDataSerializable.class);
        assertEquals(SampleIdentifiedDataSerializable.class, serializer.getClazz());
    }

    @Test
    public void testLoad_withParametrizedConstructorAndCompatibilitySwitchOn() {
        String propName = "hecaton.compat.serializers.use.default.constructor.only";
        String origProperty = System.getProperty(propName);
        try {
            System.setProperty(propName, "true");
            SerializerConfig serializerConfig = new SerializerConfig();
            serializerConfig.setClassName("uk.org.hecaton.internal.serialization.impl.TestSerializerHook$TestSerializerWithTypeConstructor");
            serializerConfig.setTypeClassName("uk.org.hecaton.internal.serialization.impl.SampleIdentifiedDataSerializable");

            SerializationConfig serializationConfig = getConfig().getSerializationConfig();
            serializationConfig.addSerializerConfig(serializerConfig);

            SerializerHookLoader hook = new SerializerHookLoader(serializationConfig, classLoader);
            Map<Class, Object> serializers = hook.getSerializers();

            TestSerializerHook.TestSerializerWithTypeConstructor serializer
                    = (TestSerializerHook.TestSerializerWithTypeConstructor)
                    serializers.get(SampleIdentifiedDataSerializable.class);
            assertNull(serializer.getClazz());
        } finally {
            if (origProperty != null) {
                System.setProperty(propName, origProperty);
            } else {
                System.clearProperty(propName);
            }
        }
    }

    @Test(expected = HecatonSerializationException.class)
    public void testLoad_implException() {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setClassName("NOT FOUND CLASS");
        serializerConfig.setTypeClassName("uk.org.hecaton.internal.serialization.impl.SampleIdentifiedDataSerializable");

        SerializationConfig serializationConfig = getConfig().getSerializationConfig();
        serializationConfig.addSerializerConfig(serializerConfig);

        new SerializerHookLoader(serializationConfig, classLoader);
    }

    @Test(expected = HecatonSerializationException.class)
    public void testLoad_typeException() {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setClassName("uk.org.hecaton.internal.serialization.impl.TestSerializerHook$TestSerializer");
        serializerConfig.setTypeClassName("NOT FOUND CLASS");

        SerializationConfig serializationConfig = getConfig().getSerializationConfig();
        serializationConfig.addSerializerConfig(serializerConfig);

        new SerializerHookLoader(serializationConfig, classLoader);
    }
}
