/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.exception;

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Set;

import static uk.org.hecaton.test.ReflectionsHelper.REFLECTIONS;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPSubsystemExceptionTest {

    @Test
    public void testAllSubclassesOverrideWrap() throws Exception {
        Set<Class<? extends CPSubsystemException>> subtypes = REFLECTIONS.getSubTypesOf(CPSubsystemException.class);
        for (Class<? extends CPSubsystemException> subtype : subtypes) {
            Method wrapMethod = subtype.getDeclaredMethod("wrap");
            assertEquals(subtype, wrapMethod.getReturnType());
        }
    }
}
