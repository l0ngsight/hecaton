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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks that a history is linearizable with respect to a {@link Model}: the Wing and Gong search
 * with Lowe's memoization of (linearized operations, state) pairs, as in Porcupine. Operations on
 * different keys are independent, so each key's sub-history is checked separately.
 */
public final class LinearizabilityChecker {

    private LinearizabilityChecker() {
    }

    /**
     * @return the keys whose sub-histories are not linearizable; empty if the history is linearizable
     */
    public static <S> List<Object> nonLinearizableKeys(Model<S> model, List<Operation> history) {
        Map<Object, List<Operation>> byKey = new LinkedHashMap<>();
        for (Operation op : history) {
            byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        }
        List<Object> failed = new ArrayList<>();
        for (Map.Entry<Object, List<Operation>> e : byKey.entrySet()) {
            if (!isLinearizable(model, e.getValue())) {
                failed.add(e.getKey());
            }
        }
        return failed;
    }

    /**
     * Checks a history of one key.
     */
    @SuppressWarnings("unchecked")
    public static <S> boolean isLinearizable(Model<S> model, List<Operation> history) {
        int n = history.size();
        Entry head = buildEntries(history);
        Set<Memo> cache = new HashSet<>();
        Deque<Object[]> calls = new ArrayDeque<>();
        BitSet linearized = new BitSet(n);
        S state = model.init();
        Entry entry = head.next;
        while (head.next != null) {
            if (entry.isCall) {
                Object next = model.step(state, entry.op.input(), entry.op.output());
                if (next != Model.REJECTED) {
                    BitSet candidate = (BitSet) linearized.clone();
                    candidate.set(entry.index);
                    if (cache.add(new Memo(candidate, next))) {
                        calls.push(new Object[]{entry, state});
                        state = (S) next;
                        linearized.set(entry.index);
                        entry.lift();
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                if (calls.isEmpty()) {
                    return false;
                }
                Object[] top = calls.pop();
                Entry call = (Entry) top[0];
                state = (S) top[1];
                linearized.clear(call.index);
                call.unlift();
                entry = call.next;
            }
        }
        return true;
    }

    /**
     * Builds the doubly linked list of call and return events in time order. At equal times calls
     * come first, so operations that touch are treated as concurrent.
     */
    private static Entry buildEntries(List<Operation> history) {
        List<Entry> events = new ArrayList<>(history.size() * 2);
        for (int i = 0; i < history.size(); i++) {
            Operation op = history.get(i);
            Entry call = new Entry(op, i, true, op.call());
            Entry ret = new Entry(op, i, false, op.ret());
            call.match = ret;
            events.add(call);
            events.add(ret);
        }
        events.sort(Comparator.comparingLong((Entry e) -> e.time).thenComparing(e -> !e.isCall));
        Entry head = new Entry(null, -1, false, Long.MIN_VALUE);
        Entry prev = head;
        for (Entry e : events) {
            prev.next = e;
            e.prev = prev;
            prev = e;
        }
        return head;
    }

    private static final class Entry {
        final Operation op;
        final int index;
        final boolean isCall;
        final long time;
        Entry match;
        Entry prev;
        Entry next;

        Entry(Operation op, int index, boolean isCall, long time) {
            this.op = op;
            this.index = index;
            this.isCall = isCall;
            this.time = time;
        }

        /** Removes this call and its return from the list. */
        void lift() {
            prev.next = next;
            next.prev = prev;
            match.prev.next = match.next;
            if (match.next != null) {
                match.next.prev = match.prev;
            }
        }

        /** Puts this call and its return back. */
        void unlift() {
            match.prev.next = match;
            if (match.next != null) {
                match.next.prev = match;
            }
            prev.next = this;
            next.prev = this;
        }
    }

    private record Memo(BitSet linearized, Object state) {
        @Override
        public boolean equals(Object o) {
            return o instanceof Memo m && linearized.equals(m.linearized) && Objects.equals(state, m.state);
        }

        @Override
        public int hashCode() {
            return 31 * linearized.hashCode() + Objects.hashCode(state);
        }
    }
}
