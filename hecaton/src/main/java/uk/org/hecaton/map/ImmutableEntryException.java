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

package uk.org.hecaton.map;

import java.io.Serial;

/**
 * Thrown when an operation would change, remove or insert again an entry of an immutable map
 * ({@link uk.org.hecaton.config.MapConfig#setImmutable(boolean)}, load protection, F23). Immutable
 * maps are insert-only: a key can be inserted once and is then never changed or removed. The
 * stored value is left as it was.
 * <p>
 * An application that retries an insert after a timeout can receive this exception for its own
 * earlier attempt that did succeed; it can treat it as "already inserted", or insert with
 * {@link IMap#putIfAbsent(Object, Object)}, which returns the stored value.
 */
public class ImmutableEntryException extends IllegalStateException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ImmutableEntryException(String message) {
        super(message);
    }

    public ImmutableEntryException(String message, Throwable cause) {
        super(message, cause);
    }
}
