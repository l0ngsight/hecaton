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

package uk.org.hecaton.buildutils;

import aQute.lib.osgi.Instruction;
import uk.org.hecaton.buildutils.HecatonManifestTransformer.InstructionDefinition;
import uk.org.hecaton.buildutils.HecatonManifestTransformer.PackageDefinition;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InstructionDefinitionTest {

    private InstructionDefinition instructionDefinition;

    @Before
    public void setUp() {
        PackageDefinition definition = new PackageDefinition("packageName", true, "version", Collections.emptySet());
        Instruction instruction = mock(Instruction.class);

        instructionDefinition = new InstructionDefinition(definition, instruction);
    }

    @Test
    public void testToString() {
        assertNotNull(instructionDefinition.toString());
        assertTrue(instructionDefinition.toString().contains("InstructionDefinition"));
    }
}
