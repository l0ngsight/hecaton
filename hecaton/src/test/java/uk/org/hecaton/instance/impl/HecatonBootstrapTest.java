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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Jet;
import uk.org.hecaton.jet.JetInstance;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.test.AssertionSinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonBootstrapTest {

    @AfterClass
    public static void teardown() throws NoSuchFieldException, IllegalAccessException {
        Hecaton.bootstrappedInstance().shutdown();
        cleanUpHecatonBootstrapSupplier();
    }

    private static void cleanUpHecatonBootstrapSupplier() {
        // Set the static instance supplier field of HecatonBootstrap
        // to null. Because of the lifetime of this field spans many
        // test classes run on the same JVM, HecatonBootstrapTest
        // and HecatonCommandLineTest were interfering with each
        // other before this cleanup step added.
        // See: https://github.com/hazelcast/hazelcast/issues/18725
        HecatonBootstrap.resetRemembered();
    }

    @Test
    public void testHecaton_bootstrappedInstance() {
        HecatonInstance hz = Hecaton.bootstrappedInstance();
        JetService jet = hz.getJet();
        executeWithBootstrappedInstance(jet);
    }

    @Test
    public void testJet_bootstrappedInstance() {
        JetInstance jet = Jet.bootstrappedInstance();
        executeWithBootstrappedInstance(jet);
    }


    public void executeWithBootstrappedInstance(JetService jet) {
        List<Integer> expected = Arrays.asList(1, 2, 3);
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3))
                .writeTo(AssertionSinks.assertAnyOrder(expected));
        jet.newJob(p).join();
    }
}
