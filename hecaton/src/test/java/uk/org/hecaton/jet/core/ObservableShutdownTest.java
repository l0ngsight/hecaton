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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.Observable;
import uk.org.hecaton.jet.function.Observer;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.StreamStage;
import uk.org.hecaton.jet.pipeline.test.SimpleEvent;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ObservableShutdownTest extends JetTestSupport {

    private static final int MEMBER_COUNT = 3;

    private HecatonInstance[] members;
    private HecatonInstance client;

    private Observable<Long> memberObservable;
    private Observable<Long> clientObservable;

    private TestObserver memberObserver;
    private TestObserver clientObserver;

    @Before
    public void before() {
        members = createHecatonInstances(MEMBER_COUNT);
        client = createHecatonClient();

        memberObserver = new TestObserver();
        memberObservable = members[members.length - 1].getJet().newObservable();
        memberObservable.addObserver(memberObserver);

        clientObserver = new TestObserver();
        clientObservable = client.getJet().newObservable();
        clientObservable.addObserver(clientObserver);
    }

    @Test
    public void when_jetInstanceIsShutDown_then_ObservablesStopReceivingEvents() {
        Pipeline pipeline = Pipeline.create();
        StreamStage<Long> stage = pipeline.readFrom(TestSources.itemStream(100))
                .withoutTimestamps()
                .map(SimpleEvent::sequence);

        stage.writeTo(Sinks.observable(clientObservable));
        stage.writeTo(Sinks.observable(memberObservable));

        //when
        Job job = client.getJet().newJob(pipeline);
        //then
        assertTrueEventually(() -> assertTrue(clientObserver.getNoOfValues() > 10));
        assertTrueEventually(() -> assertTrue(memberObserver.getNoOfValues() > 10));

        //when
        client.shutdown();
        //then
        assertObserverStopsReceivingValues(clientObserver);

        //when
        long jobId = job.getId();
        members[members.length - 1].shutdown();
        //then
        Job job1 = members[0].getJet().getJob(jobId);
        assertThat(job1).eventuallyHasStatus(JobStatus.RUNNING);
        assertObserverStopsReceivingValues(memberObserver);
    }

    private void assertObserverStopsReceivingValues(TestObserver observer) {
        assertTrueEventually(() -> {
            int values1 = observer.getNoOfValues();
            MILLISECONDS.sleep(1000);
            int values2 = observer.getNoOfValues();
            assertEquals(values1, values2);
        });
    }

    private static final class TestObserver implements Observer<Long> {

        private final AtomicInteger values = new AtomicInteger();

        @Override
        public void onNext(@Nonnull Long value) {
            values.incrementAndGet();
        }

        @Override
        public void onError(@Nonnull Throwable throwable) {
            fail("Errors aren't expected: " + throwable.getMessage());
        }

        @Override
        public void onComplete() {
            fail("Completions aren't expected");
        }

        int getNoOfValues() {
            return values.get();
        }
    }

}
