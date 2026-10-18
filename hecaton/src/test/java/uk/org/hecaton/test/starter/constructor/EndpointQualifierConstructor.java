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

import java.lang.reflect.Method;

import static uk.org.hecaton.test.starter.HecatonProxyFactory.proxyArgumentsIfNeeded;
import static uk.org.hecaton.test.starter.ReflectionUtils.getFieldValueReflectively;

@HecatonStarterConstructor(classNames = {"uk.org.hecaton.instance.EndpointQualifier"})
public class EndpointQualifierConstructor extends AbstractStarterObjectConstructor {

    public EndpointQualifierConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {

        ClassLoader targetClassLoader = targetClass.getClassLoader();

        Object protocolType = getFieldValueReflectively(delegate, "type");
        Object identifier = getFieldValueReflectively(delegate, "identifier");
        Class<?> protocolTypeClass = targetClassLoader.loadClass("uk.org.hecaton.instance.ProtocolType");

        Method resolveMethod = targetClass.getDeclaredMethod("resolve", protocolTypeClass, String.class);
        Object[] args = new Object[] {protocolType, identifier};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, targetClassLoader);
        return resolveMethod.invoke(null, proxiedArgs);
    }
}
