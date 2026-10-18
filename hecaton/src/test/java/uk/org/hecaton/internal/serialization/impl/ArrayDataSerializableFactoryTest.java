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

package uk.org.hecaton.internal.serialization.impl;

import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ArrayDataSerializableFactoryTest {
    @Test
    public void testCreate() {
        Supplier<IdentifiedDataSerializable>[] constructorFunctions = new Supplier[1];

        constructorFunctions[0] = SampleIdentifiedDataSerializable::new;

        ArrayDataSerializableFactory factory = new ArrayDataSerializableFactory(constructorFunctions);

        assertNull(factory.create(-1));
        assertNull(factory.create(1));
        assertThat(factory.create(0)).isInstanceOf(SampleIdentifiedDataSerializable.class);
    }

    @Test
    public void testCreateWithoutVersion() {
        Supplier<IdentifiedDataSerializable>[] constructorFunctions = new Supplier[1];
        Supplier<IdentifiedDataSerializable> function = mock(Supplier.class);
        constructorFunctions[0] = function;

        ArrayDataSerializableFactory factory = new ArrayDataSerializableFactory(constructorFunctions);
        factory.create(0);

        verify(function, times(1)).get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionWithEmptyConstructorFunctions() {
        new ArrayDataSerializableFactory(new Supplier[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionWithNullConstructorFunctions() {
        new ArrayDataSerializableFactory(null);
    }
}
