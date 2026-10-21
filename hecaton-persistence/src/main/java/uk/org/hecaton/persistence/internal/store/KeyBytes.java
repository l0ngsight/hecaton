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
package uk.org.hecaton.persistence.internal.store;

import java.util.Arrays;

/** A key in the index of a {@link PartitionLog}: the bytes, compared by content. */
final class KeyBytes {

    final byte[] bytes;
    private final int hash;

    KeyBytes(byte[] bytes) {
        this.bytes = bytes;
        this.hash = Arrays.hashCode(bytes);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof KeyBytes other && hash == other.hash && Arrays.equals(bytes, other.bytes);
    }
}
