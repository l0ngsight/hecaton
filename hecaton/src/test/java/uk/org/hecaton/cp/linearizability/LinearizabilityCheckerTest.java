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

import uk.org.hecaton.cp.linearizability.MapKeyModel.Input;
import uk.org.hecaton.cp.linearizability.MapKeyModel.Type;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LinearizabilityCheckerTest {

    private final MapKeyModel model = new MapKeyModel();
    private final List<Operation> history = new ArrayList<>();

    @Test
    public void sequentialHistory() {
        op(Type.SET, "a", null, 0, 1);
        op(Type.GET, null, "a", 2, 3);
        op(Type.COMPARE_AND_SET, "b", true, 4, 5, "a");
        op(Type.GET, null, "b", 6, 7);
        assertTrue(LinearizabilityChecker.isLinearizable(model, history));
    }

    @Test
    public void staleReadAfterWriteCompleted() {
        op(Type.SET, "a", null, 0, 1);
        op(Type.SET, "b", null, 2, 3);
        op(Type.GET, null, "a", 4, 5);
        assertFalse(LinearizabilityChecker.isLinearizable(model, history));
    }

    @Test
    public void concurrentReadMaySeeEitherValue() {
        op(Type.SET, "a", null, 0, 1);
        op(Type.SET, "b", null, 2, 10);
        op(Type.GET, null, "a", 3, 4);
        op(Type.GET, null, "b", 5, 6);
        assertTrue(LinearizabilityChecker.isLinearizable(model, history));
    }

    @Test
    public void readsMustNotGoBackInTime() {
        op(Type.SET, "a", null, 0, 1);
        op(Type.SET, "b", null, 2, 10);
        op(Type.GET, null, "b", 3, 4);
        op(Type.GET, null, "a", 5, 6);
        assertFalse(LinearizabilityChecker.isLinearizable(model, history));
    }

    @Test
    public void unknownWriteMayTakeEffectLate() {
        op(Type.SET, "a", null, 0, 1);
        op(Type.SET, "b", Operation.UNKNOWN, 2, Long.MAX_VALUE);
        op(Type.GET, null, "a", 3, 4);
        op(Type.GET, null, "b", 100, 101);
        assertTrue(LinearizabilityChecker.isLinearizable(model, history));
    }

    @Test
    public void unknownWriteMayNotHappen() {
        op(Type.SET, "a", null, 0, 1);
        op(Type.SET, "b", Operation.UNKNOWN, 2, Long.MAX_VALUE);
        op(Type.GET, null, "a", 100, 101);
        assertTrue(LinearizabilityChecker.isLinearizable(model, history));
    }

    @Test
    public void lostUpdateIsDetected() {
        // two compare-and-sets from the same value both succeed
        op(Type.SET, "a", null, 0, 1);
        op(Type.COMPARE_AND_SET, "b", true, 2, 5, "a");
        op(Type.COMPARE_AND_SET, "c", true, 3, 6, "a");
        assertFalse(LinearizabilityChecker.isLinearizable(model, history));
    }

    @Test
    public void putReturnsPreviousValue() {
        op(Type.PUT, "a", null, 0, 1);
        op(Type.PUT, "b", "a", 2, 3);
        op(Type.REMOVE, null, "b", 4, 5);
        op(Type.PUT_IF_ABSENT, "c", null, 6, 7);
        op(Type.PUT_IF_ABSENT, "d", "c", 8, 9);
        assertTrue(LinearizabilityChecker.isLinearizable(model, history));
    }

    @Test
    public void keysAreCheckedIndependently() {
        history.add(new Operation(0, "k1", new Input(Type.SET, "a", null), null, 0, 1));
        history.add(new Operation(1, "k2", new Input(Type.GET, null, null), "a", 2, 3));
        assertEquals(List.of("k2"), LinearizabilityChecker.nonLinearizableKeys(model, history));
    }

    @Test
    public void atomicLongModel() {
        AtomicLongModel longs = new AtomicLongModel();
        List<Operation> ops = List.of(
                new Operation(0, "c", new AtomicLongModel.Input(AtomicLongModel.Type.ADD_AND_GET, 1, 0), 1L, 0, 10),
                new Operation(1, "c", new AtomicLongModel.Input(AtomicLongModel.Type.ADD_AND_GET, 2, 0), 3L, 1, 11),
                new Operation(2, "c", new AtomicLongModel.Input(AtomicLongModel.Type.GET, 0, 0), 3L, 12, 13));
        assertTrue(LinearizabilityChecker.isLinearizable(longs, ops));
        List<Operation> bad = List.of(ops.get(0), ops.get(1),
                new Operation(2, "c", new AtomicLongModel.Input(AtomicLongModel.Type.GET, 0, 0), 1L, 12, 13));
        assertFalse(LinearizabilityChecker.isLinearizable(longs, bad));
    }

    private void op(Type type, String value, Object output, long call, long ret) {
        op(type, value, output, call, ret, null);
    }

    private void op(Type type, String value, Object output, long call, long ret, String expected) {
        history.add(new Operation(history.size(), "k", new Input(type, value, expected), output, call, ret));
    }
}
