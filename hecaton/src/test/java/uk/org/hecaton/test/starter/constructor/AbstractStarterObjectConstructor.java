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

import uk.org.hecaton.internal.util.ConstructorFunction;

import java.lang.invoke.MethodHandles;

import static uk.org.hecaton.test.starter.HecatonStarterUtils.rethrowGuardianException;
import static uk.org.hecaton.test.starter.HecatonStarterUtils.transferThrowable;

/**
 * Abstract superclass for {@link ConstructorFunction}s which, given a target
 * {@link Class}, create an {@link Object} of target {@link Class} off an input
 * {@link Object}.
 * <p>
 * For example, assuming a {@link uk.org.hecaton.config.Config} instance in the
 * current classloader, the appropriate {@link ConstructorFunction} would
 * create a {@link uk.org.hecaton.config.Config} object representing the same
 * configuration for the classloader that loads Hecaton version 3.8.
 */
public abstract class AbstractStarterObjectConstructor implements ConstructorFunction<Object, Object> {

    static final MethodHandles.Lookup PUBLIC_LOOKUP = MethodHandles.publicLookup();

    final Class<?> targetClass;

    AbstractStarterObjectConstructor(Class<?> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public Object createNew(Object arg) {
        try {
            return createNew0(arg);
        } catch (Exception e) {
            throw rethrowGuardianException(transferThrowable(e));
        }
    }

    abstract Object createNew0(Object delegate) throws Exception;
}
