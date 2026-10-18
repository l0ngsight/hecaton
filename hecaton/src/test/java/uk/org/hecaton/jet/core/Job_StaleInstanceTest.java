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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.core.TestProcessors.NoOutputSourceP;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Optional;

import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.core.JobStatus.RUNNING;
import static uk.org.hecaton.jet.impl.util.ExceptionUtil.rethrow;

/**
 * This class tests Job operations when a master doesn't know of the job
 * because it restarted. All tests check that it fails as it should.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Job_StaleInstanceTest extends JetTestSupport {

    private static TestHecatonFactory instanceFactory;
    private static HecatonInstance client;
    private static Job job;

    @BeforeClass
    public static void beforeClass() {
        TestProcessors.reset(1);
        instanceFactory = new TestHecatonFactory();
        HecatonInstance instance = instanceFactory.newHecatonInstance(smallInstanceConfig());
        DAG dag = new DAG();
        dag.newVertex("v", () -> new NoOutputSourceP());
        client = instanceFactory.newHecatonClient();
        job = client.getJet().newJob(dag);
        assertThat(job).eventuallyHasStatus(RUNNING);

        instance.getLifecycleService().terminate();
        instance = instanceFactory.newHecatonInstance(smallInstanceConfig());
        assertEqualsEventually(() -> firstItem(client.getCluster().getMembers())
                        .map(Member::getAddress).orElse(null),
                instance.getCluster().getLocalMember().getAddress());
    }

    private static <E> Optional<E> firstItem(Iterable<E> iterable) {
        Iterator<E> iterator = iterable.iterator();
        if (!iterator.hasNext()) {
            return Optional.empty();
        }
        return Optional.of(iterator.next());
    }

    @AfterClass
    public static void afterClass() {
        TestProcessors.reset(1);
        instanceFactory.terminateAll();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_restart() {
        job.restart();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_suspend() {
        job.suspend();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_resume() {
        job.resume();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_cancel() {
        job.cancel();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_getStatus() {
        job.getStatus();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_getFuture() {
        try {
            job.getFuture().get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Test(expected = JobNotFoundException.class)
    public void when_submissionTime() {
        job.getSubmissionTime();
    }

    @Test(expected = JobNotFoundException.class)
    public void when_join() {
        try {
            job.join();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
