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
package uk.org.hecaton.cp.linearizability;

/**
 * Sequential specification of an object, used by {@link LinearizabilityChecker}.
 * States must implement {@code equals} and {@code hashCode}.
 *
 * @param <S> state type
 */
public interface Model<S> {

    S init();

    /**
     * Applies the operation to {@code state}.
     *
     * @param output the operation's output, or {@link Operation#UNKNOWN}, which any state accepts
     * @return the new state, or {@link #REJECTED} if the operation cannot return
     *         {@code output} in {@code state}
     */
    Object step(S state, Object input, Object output);

    /** Returned by {@link #step} when the output is impossible in the state. */
    Object REJECTED = new Object();
}
