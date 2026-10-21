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

import java.util.Objects;
import java.util.Optional;

/**
 * Sequential specification of one key of a {@link uk.org.hecaton.cp.CPMap}. The state is the
 * value, {@code Optional.empty()} when the key is absent.
 */
public final class MapKeyModel implements Model<Optional<String>> {

    /** The operations on a key. */
    public enum Type { GET, PUT, SET, PUT_IF_ABSENT, REMOVE, DELETE, COMPARE_AND_SET }

    /**
     * An operation's input.
     *
     * @param type     the operation
     * @param value    the value written, if any
     * @param expected the expected value of compare-and-set
     */
    public record Input(Type type, String value, String expected) {
        @Override
        public String toString() {
            return type + (value != null ? "(" + (expected != null ? expected + "->" : "") + value + ")" : "");
        }
    }

    @Override
    public Optional<String> init() {
        return Optional.empty();
    }

    @Override
    public Object step(Optional<String> state, Object in, Object output) {
        Input input = (Input) in;
        String current = state.orElse(null);
        boolean known = output != Operation.UNKNOWN;
        return switch (input.type()) {
            case GET -> known && !Objects.equals(output, current) ? REJECTED : state;
            case PUT -> known && !Objects.equals(output, current) ? REJECTED : Optional.of(input.value());
            case SET -> Optional.of(input.value());
            case PUT_IF_ABSENT -> known && !Objects.equals(output, current) ? REJECTED
                    : current == null ? Optional.of(input.value()) : state;
            case REMOVE -> known && !Objects.equals(output, current) ? REJECTED : Optional.empty();
            case DELETE -> Optional.empty();
            case COMPARE_AND_SET -> {
                boolean matches = input.expected().equals(current);
                if (known && (Boolean) output != matches) {
                    yield REJECTED;
                }
                yield matches ? Optional.of(input.value()) : state;
            }
        };
    }
}
