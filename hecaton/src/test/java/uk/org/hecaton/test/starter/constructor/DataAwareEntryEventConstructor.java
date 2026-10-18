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

package uk.org.hecaton.test.starter.constructor;

import uk.org.hecaton.test.starter.HecatonStarterConstructor;

import java.lang.reflect.Constructor;

import static uk.org.hecaton.test.starter.HecatonProxyFactory.proxyArgumentsIfNeeded;
import static uk.org.hecaton.test.starter.ReflectionUtils.callNoArgMethod;
import static uk.org.hecaton.test.starter.ReflectionUtils.getDelegateFromProxyClass;
import static uk.org.hecaton.test.starter.ReflectionUtils.getFieldValueReflectively;

@HecatonStarterConstructor(classNames = {"uk.org.hecaton.map.impl.DataAwareEntryEvent"})
public class DataAwareEntryEventConstructor extends AbstractStarterObjectConstructor {

    public DataAwareEntryEventConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        // locate required classes on target class loader
        ClassLoader starterClassLoader = targetClass.getClassLoader();
        Class<?> dataClass = starterClassLoader.loadClass("uk.org.hecaton.internal.serialization.Data");
        Class<?> memberClass = starterClassLoader.loadClass("uk.org.hecaton.cluster.Member");
        Class<?> serServiceClass = starterClassLoader.loadClass("uk.org.hecaton.internal.serialization.SerializationService");
        Constructor<?> constructor = targetClass.getConstructor(memberClass, Integer.TYPE, String.class, dataClass,
                dataClass, dataClass, dataClass, serServiceClass);
        Object unproxiedDelegate = getDelegateFromProxyClass(delegate);
        Object serializationService = getFieldValueReflectively(delegate, "serializationService");
        Object source = callNoArgMethod(unproxiedDelegate, "getSource");
        Object member = getFieldValueReflectively(delegate, "member");
        Object entryEventType = getFieldValueReflectively(delegate, "entryEventType");
        Integer eventTypeId = (Integer) callNoArgMethod(entryEventType, "getType");
        Object dataKey = getFieldValueReflectively(delegate, "dataKey");
        Object dataNewValue = getFieldValueReflectively(delegate, "dataNewValue");
        Object dataOldValue = getFieldValueReflectively(delegate, "dataOldValue");
        Object dataMergingValue = getFieldValueReflectively(delegate, "dataMergingValue");

        Object[] args = new Object[]{
                member,
                eventTypeId,
                source,
                dataKey,
                dataNewValue,
                dataOldValue,
                dataMergingValue,
                serializationService,
        };

        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);
        return constructor.newInstance(proxiedArgs);
    }
}
