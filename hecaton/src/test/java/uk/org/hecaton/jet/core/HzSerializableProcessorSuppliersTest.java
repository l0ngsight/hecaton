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

package uk.org.hecaton.jet.core;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.net.URLClassLoader;

import static java.util.Objects.requireNonNull;

@Category({QuickTest.class, ParallelJVMTest.class})
public class HzSerializableProcessorSuppliersTest extends SimpleTestInClusterSupport {

    private final DAG dag = new DAG();
    private final URL url = requireNonNull(
            getClass().getResource("DataSerializableSuppliers.jar"));


    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        initialize(2, config);
    }

    @Test
    public void test_metaSupplier() throws Exception {
        try (URLClassLoader cl = new URLClassLoader(new URL[]{url})) {
            ProcessorMetaSupplier metaSupplier =
                    ClassLoaderUtil.newInstance(cl, "com.example.DataSerializableSuppliers$MetaSupplier");
            dag.newVertex("v", metaSupplier);
            submitJob();
        }
    }

    @Test
    public void test_pSupplier() throws Exception {
        try (URLClassLoader cl = new URLClassLoader(new URL[]{url})) {
            ProcessorSupplier pSupplier =
                    ClassLoaderUtil.newInstance(cl, "com.example.DataSerializableSuppliers$PSupplier");
            dag.newVertex("v", pSupplier);
            submitJob();
        }
    }

    @Test
    public void test_simpleSupplier() throws Exception {
        try (URLClassLoader cl = new URLClassLoader(new URL[]{url})) {
            SupplierEx<Processor> supplier =
                    ClassLoaderUtil.newInstance(cl, "com.example.DataSerializableSuppliers$SimpleSupplier");
            dag.newVertex("v", supplier);
            submitJob();
        }
    }

    private void submitJob() {
        JobConfig config = new JobConfig();
        config.addJar(url);
        instances()[1].getJet().newJob(dag, config).join();
    }
}
