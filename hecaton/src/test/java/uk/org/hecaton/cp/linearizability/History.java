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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Records operations from many threads. An operation that fails or times out has an unknown
 * outcome: it may or may not have taken effect, now or later.
 */
public final class History {

    private final AtomicInteger ids = new AtomicInteger();
    private final List<Operation> operations = new ArrayList<>();
    private final AtomicInteger unknown = new AtomicInteger();

    /**
     * Runs {@code action} and records it.
     *
     * @param readOnly a read whose outcome is unknown is not recorded, as it changed nothing
     * @return the output, or {@link Operation#UNKNOWN}
     */
    public Object record(Object key, Object input, boolean readOnly, Callable<Object> action) {
        long call = System.nanoTime();
        Object output;
        long ret;
        try {
            output = action.call();
            ret = System.nanoTime();
        } catch (Throwable t) {
            if (t instanceof Error e && !(t instanceof AssertionError)) {
                throw e;
            }
            unknown.incrementAndGet();
            if (readOnly) {
                return Operation.UNKNOWN;
            }
            output = Operation.UNKNOWN;
            ret = Long.MAX_VALUE;
        }
        Operation op = new Operation(ids.getAndIncrement(), key, input, output, call, ret);
        synchronized (operations) {
            operations.add(op);
        }
        return output;
    }

    public List<Operation> operations() {
        synchronized (operations) {
            return new ArrayList<>(operations);
        }
    }

    public int unknownCount() {
        return unknown.get();
    }
}
