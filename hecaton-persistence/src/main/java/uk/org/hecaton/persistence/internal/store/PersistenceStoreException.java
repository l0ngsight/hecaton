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

import uk.org.hecaton.core.HecatonException;

/** Failure of the persistent store: an I/O error, or data on disk that cannot be read. */
public class PersistenceStoreException extends HecatonException {

    public PersistenceStoreException(String message) {
        super(message);
    }

    public PersistenceStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
