/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.instance.impl;

/**
 * Members run on Java 25 or later; the client library, in the same jar, supports Java 17 and
 * later (spec §8.2).
 */
public final class MemberJavaVersion {

    /** The lowest Java feature release a member runs on. */
    public static final int MINIMUM = 25;

    /** Why a member does not start on an older Java. */
    public static final String MESSAGE = "Hecaton members require Java " + MINIMUM
            + "+; the client library supports Java 17+";

    private MemberJavaVersion() {
    }

    /** Fails unless this JVM can run a member. */
    public static void check() {
        check(Runtime.version().feature());
    }

    static void check(int javaFeatureVersion) {
        if (javaFeatureVersion < MINIMUM) {
            throw new IllegalStateException(MESSAGE + " (this is Java " + javaFeatureVersion + ")");
        }
    }
}
