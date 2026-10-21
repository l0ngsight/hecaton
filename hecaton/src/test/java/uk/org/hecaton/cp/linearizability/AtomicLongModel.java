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
 * Sequential specification of an {@link uk.org.hecaton.cp.IAtomicLong}.
 */
public final class AtomicLongModel implements Model<Long> {

    /** The operations. */
    public enum Type { GET, ADD_AND_GET, COMPARE_AND_SET }

    /**
     * An operation's input.
     *
     * @param type     the operation
     * @param value    the delta or new value
     * @param expected the expected value of compare-and-set
     */
    public record Input(Type type, long value, long expected) {
    }

    @Override
    public Long init() {
        return 0L;
    }

    @Override
    public Object step(Long state, Object in, Object output) {
        Input input = (Input) in;
        boolean known = output != Operation.UNKNOWN;
        return switch (input.type()) {
            case GET -> known && !state.equals(output) ? REJECTED : state;
            case ADD_AND_GET -> {
                long next = state + input.value();
                yield known && !Long.valueOf(next).equals(output) ? REJECTED : next;
            }
            case COMPARE_AND_SET -> {
                boolean matches = state == input.expected();
                if (known && (Boolean) output != matches) {
                    yield REJECTED;
                }
                yield matches ? input.value() : state;
            }
        };
    }
}
