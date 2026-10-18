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

package uk.org.hecaton.test.annotation;

import uk.org.hecaton.test.TestEnvironment;
import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Mark a test as a compatibility test, suitable to be executed on a cluster with mixed Hecaton versions.
 * To actually perform a compatibility test, you need to start test execution with system property
 * "hecaton.test.compatibility=true".
 * <p>When executing a compatibility test, there are limitations, the most important being:
 * <ul>
 * <li>Only use public API. Code that relies on Hecaton internals, like obtaining a reference to the {@code Node}
 * in a {@code HecatonInstance}, will only succeed when executed on a {@code HecatonInstance} of the current
 * version.</li>
 * <li>Test classes used as domain classes, listeners, entry processors etc, when implementing {@code Serializable}
 * must specify a {@code serialVersionUID}, otherwise failures in serialization/deserialization will occur.</li>
 * </ul>
 * </p>
 *
 * @see TestEnvironment#isRunningCompatibilityTest()
 */
@Retention(RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Tag("uk.org.hecaton.test.annotation.CompatibilityTest")
public @interface CompatibilityTest {
}
