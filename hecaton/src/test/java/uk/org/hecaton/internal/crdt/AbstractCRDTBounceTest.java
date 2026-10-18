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

package uk.org.hecaton.internal.crdt;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.exception.RetryableHecatonException;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.TestThread;
import uk.org.hecaton.test.jitter.JitterRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Base test class for testing CRDT thread safety and replication behaviour.
 * The test will spawn up to {@value #CONCURRENCY} threads that will perform
 * mutation on the provided hecaton instance and it will additionally
 * "bounce" the hecaton instances. This means that it rotates through the
 * instance collection, shutting down members and replacing them with new
 * ones.
 * The mutation action may not be applied as the mutation action may
 * provide an instance which is shutting down to the
 * {@link #mutate(HecatonInstance)} method. The method does not need to
 * concern with exceptions thrown because of this - this is handled by
 * this class as well.
 * <p>
 * This should test that the strong consistency property of the CRDT holds
 * under concurrent mutation and instance shutdown.
 */
public abstract class AbstractCRDTBounceTest extends HecatonTestSupport {

    private static final int CONCURRENCY = 10;
    private static final int NODE_COUNT = 3;

    @Rule
    public JitterRule jitterRule = new JitterRule();

    private final AtomicBoolean testStop = new AtomicBoolean();

    private AtomicReferenceArray<HecatonInstance> instances;
    private Future bouncingFuture;

    @Before
    public void setup() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        instances = new AtomicReferenceArray<>(NODE_COUNT);
        for (int i = 0; i < NODE_COUNT; i++) {
            instances.set(i, factory.newHecatonInstance(getConfig()));
        }
        bouncingFuture = startBouncing(instances, testStop, factory);
    }

    @Test
    public void mutateAndBounceMembersProducesCorrectResult() throws ExecutionException, InterruptedException {
        final TestThread[] mutationThreads = new TestThread[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            final TestThread mutationThread = new MutationThread(instances, testStop);
            mutationThread.start();
            mutationThreads[i] = mutationThread;
        }

        sleepAndStop(testStop, TimeUnit.MINUTES.toSeconds(1));

        bouncingFuture.get();
        for (TestThread mutationThread : mutationThreads) {
            mutationThread.assertSucceedsEventually();
        }

        for (int i = 0; i < instances.length(); i++) {
            assertState(instances.get(i));
        }
    }

    /**
     * Asserts that the state of the hecaton instance is equal to the
     * expected state. This method is called when the test is done mutating
     * the state and bouncing the members.
     *
     * @param hecatonInstance the instance on which to assert the state
     */
    protected abstract void assertState(HecatonInstance hecatonInstance);

    /**
     * Returns the logger for the concrete implementation of this test class.
     */
    protected abstract ILogger getLogger();

    /**
     * Performs a mutation action on the CRDT on the provided instance.
     *
     * @param hecatonInstance the hecaton instance on which CRDT state is
     *                          mutated
     */
    protected abstract void mutate(HecatonInstance hecatonInstance);

    /**
     * The mutation thread. It rotates through the instances array invoking the
     * {@link #mutate(HecatonInstance)} method and providing a different
     * instance on each invocation.
     * The provided instance may be shutting down in which case the mutation
     * method may throw exceptions. The usual methods thrown by a shutting
     * down member are handled by this thread.
     */
    class MutationThread extends TestThread {
        private final AtomicReferenceArray<HecatonInstance> instances;
        private final AtomicBoolean stop;

        MutationThread(AtomicReferenceArray<HecatonInstance> instances, AtomicBoolean stop) {
            super("MutationThread");
            this.instances = instances;
            this.stop = stop;
        }

        @Override
        public void onError(Throwable t) {
            stop.set(true);
        }

        @Override
        public void doRun() throws Throwable {
            int index = 0;
            while (!stop.get()) {
                index = (index + 1) % instances.length();
                final HecatonInstance hecatonInstance = instances.get(index);
                try {
                    mutate(hecatonInstance);
                } catch (HecatonInstanceNotActiveException ignored) {
                } catch (RetryableHecatonException ignored) {
                } catch (Exception e) {
                    getLogger().severe("Error occurred while mutating the CRDT", e);
                }
            }
        }
    }

    /**
     * Starts the instance bouncing thread. The thread iterates through the
     * {@code instances} and shuts them down, replacing each with a new one.
     * The thread will stop once the {@code stop} boolean has been set to
     * {@code true}.
     *
     * @param instances the instances array
     * @param stop      a flag to stop the bouncing test
     * @param factory   the instance factory
     * @return a future representing the pending completion of the bouncing task
     */
    private Future startBouncing(final AtomicReferenceArray<HecatonInstance> instances,
                                 final AtomicBoolean stop,
                                 final TestHecatonInstanceFactory factory) {
        return spawn((Runnable) () -> {
            int i = 0;
            while (!stop.get()) {
                i = (i + 1) % instances.length();
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                instances.get(i).shutdown();
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                instances.set(i, factory.newHecatonInstance(getConfig()));
            }
        });
    }

    protected Config getConfig() {
        return new Config().setProperty(ClusterProperty.PARTITION_COUNT.getName(), "5");
    }
}
