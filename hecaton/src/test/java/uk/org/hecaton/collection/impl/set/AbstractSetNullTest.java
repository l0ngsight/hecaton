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

package uk.org.hecaton.collection.impl.set;

import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.internal.util.ExceptionUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.fail;

public abstract class AbstractSetNullTest extends HecatonTestSupport {

    @Test
    public void testNullability() {
        assertThrowsNPE(s -> s.contains(null));
        assertThrowsNPE(s -> s.toArray((Object[]) null));
        assertThrowsNPE(s -> s.add(null));
        assertThrowsNPE(s -> s.remove(null));
        assertThrowsNPE(s -> s.containsAll(null));
        assertThrowsNPE(s -> s.addAll(null));
        assertThrowsNPE(s -> s.removeAll(null));
        assertThrowsNPE(s -> s.retainAll(null));
        assertThrowsNPE(s -> s.addItemListener(null, true));
        assertThrowsNPE(s -> s.removeItemListener(null));
    }

    private void assertThrowsNPE(ConsumerEx<ISet<Object>> method) {
        assertThrows(NullPointerException.class, method);
    }

    private void assertThrows(Class<? extends Exception> expectedExceptionClass,
                              ConsumerEx<ISet<Object>> method) {
        try {
            method.accept(getDriver().getSet(randomName()));
            fail("Expected " + expectedExceptionClass
                    + " but there was no exception!");
        } catch (Exception e) {
            Assert.assertSame(expectedExceptionClass, e.getClass());
        }
    }

    @FunctionalInterface
    public interface ConsumerEx<T> extends Consumer<T> {
        void acceptEx(T t) throws Exception;

        @Override
        default void accept(T t) {
            try {
                acceptEx(t);
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected abstract HecatonInstance getDriver();
}
