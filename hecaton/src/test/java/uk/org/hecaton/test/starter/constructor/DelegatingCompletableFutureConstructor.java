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

import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.starter.HecatonProxyFactory;
import uk.org.hecaton.test.starter.HecatonStarterConstructor;
import uk.org.hecaton.test.starter.ReflectionUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.internal.util.ExceptionUtil.sneakyThrow;

@HecatonStarterConstructor(classNames = "uk.org.hecaton.spi.impl.DelegatingCompletableFuture")
public class DelegatingCompletableFutureConstructor extends AbstractStarterObjectConstructor {

    public DelegatingCompletableFutureConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        ClassLoader classLoader = targetClass.getClassLoader();
        Class<?> serializationServiceClass = classLoader.loadClass(SerializationService.class.getName());
        MethodType ctorType = MethodType.methodType(void.class, serializationServiceClass, CompletableFuture.class);

        MethodHandle constructor = PUBLIC_LOOKUP.findConstructor(targetClass, ctorType);

        Object serializationService = HecatonProxyFactory.proxyArgumentsIfNeeded(
                new Object[] {ReflectionUtils.getFieldValueReflectively(delegate, "serializationService")},
                serializationServiceClass.getClassLoader())[0];
        try {
            return constructor.invoke(serializationService, delegate);
        } catch (Throwable throwable) {
            throw sneakyThrow(throwable);
        }
    }
}
