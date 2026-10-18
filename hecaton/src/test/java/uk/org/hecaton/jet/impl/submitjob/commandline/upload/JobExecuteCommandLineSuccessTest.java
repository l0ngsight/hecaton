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

package uk.org.hecaton.jet.impl.submitjob.commandline.upload;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonBootstrap;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.config.JetConfig;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.JobStatus;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.getParalleJarPath;
import static uk.org.hecaton.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.jarDoesNotExistInTempDirectory;
import static org.junit.Assert.assertFalse;

// Test for HecatonCommandLine
@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class JobExecuteCommandLineSuccessTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HecatonBootstrap.resetRemembered();
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsEnabled()
            throws IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        HecatonInstance member = createCluster();

        HecatonInstance hecatonClient = createHecatonClient();

        // newJob() is called from another thread, but it succeeds
        // because CLI does not use ThreadLocal to store ExecuteJobParameters
        HecatonBootstrap.executeJarOnCLI(() -> hecatonClient,
                getParalleJarPath().toString(),
                null,
                null,
                null,
                Collections.emptyList());

        assertFalse(hecatonClient.getLifecycleService().isRunning());

        JetService jetService = member.getJet();
        assertJobIsRunning(jetService);
    }

    private HecatonInstance createCluster() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        return createHecatonInstance(config);
    }

    public static void assertJobIsRunning(JetService jetService) throws IOException {
        // Assert job size
        assertEqualsEventually(() -> jetService.getJobs().size(), 1);

        // Assert job status
        Job job = jetService.getJobs().get(0);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        // Assert job jar does is deleted
        jarDoesNotExistInTempDirectory();
    }
}
