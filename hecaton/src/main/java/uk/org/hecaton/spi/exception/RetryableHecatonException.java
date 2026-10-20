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

package uk.org.hecaton.spi.exception;

import uk.org.hecaton.core.HecatonException;

/**
 * A 'marker' exception that indicates that an operation can be retried. E.g. if map.get is sent to a partition that
 * is currently migrating, a subclass of this exception is thrown, so the caller can deal with it (e.g. sending the
 * request to the new partition owner).
 */
public class RetryableHecatonException extends HecatonException implements RetryableException {

    public RetryableHecatonException() {
    }

    public RetryableHecatonException(String message) {
        super(message);
    }

    public RetryableHecatonException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableHecatonException(Throwable cause) {
        super(cause);
    }
}
