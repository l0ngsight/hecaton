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
 * One operation of a recorded history.
 *
 * @param id     unique id within its history
 * @param key    the independent object the operation acts on (histories are checked per key)
 * @param input  what the operation did
 * @param output what it returned, or {@link #UNKNOWN} if the caller did not learn the outcome
 * @param call   when it was invoked (monotonic, nanoseconds)
 * @param ret    when it returned; {@link Long#MAX_VALUE} if the outcome is unknown, so it may
 *               take effect at any point after its call
 */
public record Operation(int id, Object key, Object input, Object output, long call, long ret) {

    /** Output of an operation whose outcome is unknown. */
    public static final Object UNKNOWN = new Object() {
        @Override
        public String toString() {
            return "UNKNOWN";
        }
    };

    public boolean outcomeKnown() {
        return output != UNKNOWN;
    }
}
