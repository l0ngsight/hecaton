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
package uk.org.hecaton.internal.serialization.impl.compact;

import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.List;
import java.util.stream.Stream;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultReflectiveSerializationRestrictionTest
        extends AbstractReflectiveSerializationRestrictionTest {

    @Parameter(1)
    public String name;

    @Override
    protected boolean isSuccessExpected() {
        return false;
    }

    @Parameters(name = "{index} class={1}")
    public static List<Object[]> parameters() {
        return Stream.of(new HeapData(new byte[12]), new Packet(new byte[12], 10))
                     .map(e -> new Object[]{e, e.getClass().getSimpleName()}).toList();
    }
}
