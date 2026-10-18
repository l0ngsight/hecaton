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

package uk.org.hecaton.internal.util;

/**
 * Utility class to set system parameters for the clock implementation.
 */
public final class ClockProperties {

    /**
     * Clock offset property in milliseconds. When it is set to a non-zero value,
     * <code>Clock.currentTimeMillis()</code> will return a shifted <code>System#currentTimeMillis()</code>
     * time by the given offset value.
     */
    public static final String HECATON_CLOCK_OFFSET = "uk.org.hecaton.clock.offset";

    /**
     * The implementation of {@link uk.org.hecaton.internal.util.Clock.ClockImpl}
     * to use. If not defined we will use
     * {@link uk.org.hecaton.internal.util.Clock.SystemClock} or
     * {@link uk.org.hecaton.internal.util.Clock.SystemOffsetClock}, depending on
     * whether an offset is defined with {@link #HECATON_CLOCK_OFFSET}.
     */
    public static final String HECATON_CLOCK_IMPL = "uk.org.hecaton.clock.impl";

    private ClockProperties() {
    }
}
