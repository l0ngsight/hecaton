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

package uk.org.hecaton.jet.impl.deployment;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceProxy;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.Util;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.JobStatus;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.jet.impl.JobClassLoaderService;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.StreamSource;
import uk.org.hecaton.jet.pipeline.test.SimpleEvent;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.JarUtil;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import uk.org.webcompere.systemstubs.rules.SystemPropertiesRule;

import java.io.File;

import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProcessorClassLoaderCleanupTest extends JetTestSupport {

    private static File jarFile;

    private JetService jet;
    private HecatonInstance member;

    @ClassRule
    public static final SystemPropertiesRule systemProperties = new SystemPropertiesRule(
            ClusterProperty.PROCESSOR_CUSTOM_LIB_DIR.getName(),
            System.getProperty("java.io.tmpdir"));

    @BeforeClass
    public static void beforeClass() throws Exception {
        jarFile = File.createTempFile("resources_", ".jar");
        JarUtil.createResourcesJarFile(jarFile);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (jarFile != null) {
            IOUtil.deleteQuietly(jarFile);
        }
    }

    @Before
    public void setUp() throws Exception {
        member = createHecatonInstance();
        jet = member.getJet();
    }

    @Test
    public void processorClassLoaderRemovedAfterJobFinished() {
        Pipeline p = Pipeline.create();

        StreamSource<SimpleEvent> source = TestSources.itemStream(1);
        p.readFrom(source).withoutTimestamps()
                .setLocalParallelism(1)
         .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        jobConfig.addCustomClasspath(source.name(), jarFile.getName());
        Job job = jet.newJob(p, jobConfig);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        JetServiceBackend jetServiceBackend =
                ((HecatonInstanceProxy) member).getOriginal().node.getNodeEngine().getService(JetServiceBackend.SERVICE_NAME);
        JobClassLoaderService jobClassLoaderService = jetServiceBackend.getJobClassLoaderService();

        ChildFirstClassLoader classLoader = (ChildFirstClassLoader) jobClassLoaderService.getProcessorClassLoader(job.getId(), source.name());

        job.suspend();
        assertThat(job).eventuallyHasStatus(JobStatus.SUSPENDED);

        assertThat(classLoader.isClosed())
                .describedAs("classloader hasn't been closed")
                .isTrue();

        assertThatThrownBy(() -> jobClassLoaderService.getProcessorClassLoader(job.getId(), source.name()))
                .isInstanceOf(HecatonException.class)
                .hasMessageContaining("JobClassLoaders for jobId=" + Util.idToString(job.getId())
                        + " requested, but it does not exists");
    }
}
