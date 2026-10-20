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

package uk.org.hecaton.internal.metrics.impl;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import uk.org.hecaton.internal.metrics.Probe;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.starter.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Stream;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeUtilsTest extends HecatonTestSupport {

    @Test
    public void testPrivateConstructor() {
        assertUtilityConstructor(ProbeUtils.class);
    }

    /** Prints the types that a @Probe has been attached to in the codebase, and where they can be found */
    public static void main(final String[] args) {
        final Multimap<String, String> probeTypeToClassesFoundIn = TreeMultimap.create();

        ReflectionUtils.getReflectionsForTestPackage("uk.org.hecaton").getSubTypesOf(Object.class).forEach(clazz -> {
            final Stream<Class<?>> methodTypes = Arrays.stream(clazz.getDeclaredMethods())
                    .filter(method -> method.getAnnotation(Probe.class) != null).map(Method::getReturnType);
            final Stream<Class<?>> fieldTypes = Arrays.stream(clazz.getDeclaredFields())
                    .filter(field -> field.getAnnotation(Probe.class) != null).map(Field::getType);

            Stream.concat(methodTypes, fieldTypes)
                    .forEach(probeType -> probeTypeToClassesFoundIn.put(probeType.getName(), clazz.getName()));
        });

        probeTypeToClassesFoundIn.asMap().entrySet().forEach(System.out::println);
    }
}
