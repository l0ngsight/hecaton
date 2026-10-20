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

package uk.org.hecaton.jet;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.PacketFiltersUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.TestClass;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static uk.org.hecaton.internal.management.ThreadDumpGenerator.dumpAllThreads;
import static uk.org.hecaton.jet.Util.idToString;
import static uk.org.hecaton.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_NAME;
import static uk.org.hecaton.jet.impl.JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE;
import static uk.org.hecaton.jet.impl.JobRepository.JOB_EXECUTION_RECORDS_MAP_NAME;
import static uk.org.hecaton.jet.impl.JobRepository.JOB_RECORDS_MAP_NAME;
import static uk.org.hecaton.jet.impl.JobRepository.JOB_RESULTS_MAP_NAME;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Base class for tests that share the cluster for all jobs. In order to use
 * shared cluster instance the subclass must call {@link #initialize} or {@link
 * #initializeWithClient} method. If any of them is not called, this class
 * behaves in the same way as {@link JetTestSupport}.
 * <p>
 * It is strongly recommended to use serial runner for tests sharing cluster
 * instance. In case of parallel execution, cleanup code ({@link
 * #supportAfter()} can conflict with test execution. But even if the test does
 * not start any Jet jobs, {@link #supportAfter()} is not safe to run in
 * parallel due to potential concurrent destruction and usage of some Jet
 * distributed objects.
 */
public abstract class SimpleTestInClusterSupport extends JetTestSupport {

    private static final ILogger SUPPORT_LOGGER = Logger.getLogger(SimpleTestInClusterSupport.class);

    private static TestHecatonFactory factory;
    private static Config config;
    private static HecatonInstance[] instances;
    private static HecatonInstance client;
    private final Supplier<Boolean> parallelExecution = uk.org.hecaton.jet.impl.util.Util.memoize(this::isParallelTestExecution);

    protected static void initialize(int memberCount, @Nullable Config config) {
        assertNoRunningInstances();

        initializeWitSupplier(memberCount, () -> config == null ? smallInstanceConfig() : config);
    }

    protected static void initializeWitSupplier(int memberCount, Supplier<Config> configSupplier) {
        assertNoRunningInstances();

        assert factory == null : "already initialized";
        factory = new TestHecatonFactory();
        instances = new HecatonInstance[memberCount];
        SimpleTestInClusterSupport.config = configSupplier.get();
        // create members
        for (int i = 0; i < memberCount; i++) {
            instances[i] = factory.newHecatonInstance(configSupplier.get());
        }
        assertEqualsEventually(() -> instance().getLifecycleService().isRunning(), true);
    }

    protected static void initializeWithClient(int memberCount,
                                               @Nullable Config config,
                                               @Nullable ClientConfig clientConfig) {

        Supplier<Config> configSupplier = () -> config == null ? smallInstanceConfig() : config;
        initializeWithClientAndConfigSupplier(memberCount, configSupplier, clientConfig);
    }

    protected static void initializeWithClientAndConfigSupplier(int memberCount,
                                                                Supplier<Config> configSupplier,
                                                                @Nullable ClientConfig clientConfig) {
        if (clientConfig == null) {
            clientConfig = new ClientConfig();
        }
        initializeWitSupplier(memberCount, configSupplier);
        client = factory.newHecatonClient(clientConfig);
    }

    protected static void assertNoLightJobsLeftEventually(HecatonInstance instance) {
        assertTrueEventually(() -> {
            List<Job> runningJobs = instance.getJet().getJobs().stream()
                    .filter(Job::isLightJob)
                    .collect(toList());
            int size = runningJobs.size();
            assertEquals("at this point no running light jobs were expected, but got: " + runningJobs, 0, size);
        });
    }

    @After
    @AfterEach
    public void supportAfter() {
        if (instances == null) {
            return;
        }
        List<HecatonInstance> stillActiveInstances = Arrays.stream(instances())
                .filter(SimpleTestInClusterSupport::testIfInstanceIsStillActive)
                .collect(toList());

        if (stillActiveInstances.isEmpty()) {
            return;
        }
        for (HecatonInstance inst : stillActiveInstances) {
            PacketFiltersUtil.resetPacketFiltersFrom(inst);
        }
        // after each test ditch all jobs and objects
        List<Job> jobs = stillActiveInstances.get(0).getJet().getJobs();
        SUPPORT_LOGGER.info("Ditching " + jobs.size() + " jobs in SimpleTestInClusterSupport.@After: " +
                jobs.stream().map(j -> idToString(j.getId())).collect(joining(", ", "[", "]")));
        for (Job job : jobs) {
            failIfParallel();
            ditchJob(job, instances());
        }

        // cancel all light jobs by cancelling their executions
        for (HecatonInstance inst : stillActiveInstances) {
            JetServiceBackend jetServiceBackend = getJetServiceBackend(inst);
            jetServiceBackend.getJobExecutionService().cancelAllExecutions("ditching all jobs after a test");
            jetServiceBackend.getJobExecutionService().waitAllExecutionsTerminated();
        }
        // If the client was created and used any proxy to a distributed object, we need to destroy that object through
        // client, so the proxy in client's internals was destroyed as well. Without going through client, if we use
        // the same distributed object in more than one test, we are not going to invoke InitializeDistributedObjectOperation
        // in all of them (just in the first one).
        Collection<DistributedObject> objects = client != null ? client.getDistributedObjects()
                : instance().getDistributedObjects();
        SUPPORT_LOGGER.info("Destroying " + objects.size()
                + " distributed objects in SimpleTestInClusterSupport.@After: "
                + objects.stream().map(o -> o.getServiceName() + "/" + String.valueOf(o.getName()))
                .collect(Collectors.joining(", ", "[", "]")));
        // Jet keeps some IMap references in JobRepository.
        // Destroying proxies removes the objects from proxy registry but JobRepository keeps
        // the instances. They can still be used but when used it will NOT recreate proxy
        // so after next test they will not be visible in getDistributedObjects()
        // and will not be cleared. Because of that we do not destroy proxies for known Jet and SQL objects
        // (but snapshots IMaps and job resources will be destroyed) but only clear the IMaps.
        //
        // This behavior affects all objects for which references are kept across test methods,
        // (both in production code and in tests) but for those there is no generic solution.
        List<String> jetMaps = List.of(JOB_RECORDS_MAP_NAME, JOB_RESULTS_MAP_NAME,
                JOB_EXECUTION_RECORDS_MAP_NAME, JOB_EXECUTION_RECORDS_MAP_NAME,
                EXPORTED_SNAPSHOTS_DETAIL_CACHE,
                SQL_CATALOG_MAP_NAME);
        for (DistributedObject o : objects) {
            if (o.getServiceName().equals(MapService.SERVICE_NAME) && jetMaps.contains(o.getName())) {
                IMap map = (IMap) o;
                if (!map.isEmpty()) {
                    // SQL might create catalog IMap during optimization tests, but it is fine
                    // in parallel execution as long as it is empty (so no actual SQL objects are created).
                    //
                    // This check is slightly racy - it may be not reported in all test methods of given class
                    // but should be reported in at least one of them.
                    failIfParallel();
                    map.clear();
                }
            } else {
                failIfParallel();
                o.destroy();
            }
        }

        for (HecatonInstance instance : instances) {
            assertTrueEventually(() -> {
                // Let's wait for all unprocessed operations (like destroying distributed object) to complete
                assertEquals(0, Accessors.getNodeEngineImpl(instance).getEventService().getEventQueueSize());
            });
        }
    }

    private void failIfParallel() {
        assertFalse("Parallel execution for SimpleTestInClusterSupport with shared instances in not supported " +
                    "if any object or job is created", parallelExecution.get());
        assertThatIsNotMultithreadedTest();
    }

    private boolean isParallelTestExecution() {
        TestClass testClass = new TestClass(this.getClass());
        return Arrays.stream(testClass.getAnnotations()).anyMatch(
                annotation ->
                        annotation instanceof RunWith rw
                                && HecatonParallelClassRunner.class.isAssignableFrom(rw.value())
                        ||
                        annotation instanceof Parameterized.UseParametersRunnerFactory uprf
                                && HecatonParallelParametersRunnerFactory.class.isAssignableFrom(
                                        uprf.value())
        );
    }

    private static boolean testIfInstanceIsStillActive(HecatonInstance instance) {
        if (instance instanceof HecatonInstanceImpl impl) {
            try {
                return impl.isRunning();
            } catch (HecatonInstanceNotActiveException ignored) {
                return false;
            }
        }
        try {
            instance.getCluster().getClusterState();
            return true;
        } catch (HecatonInstanceNotActiveException ignored) {
            return false;
        }
    }

    @AfterClass
    @AfterAll
    public static void supportAfterClass() throws Exception {
        try {
            if (factory != null) {
                SUPPORT_LOGGER.info("Terminating instance factory in SimpleTestInClusterSupport.@AfterClass");
                spawn(() -> factory.terminateAll())
                        .get(1, TimeUnit.MINUTES);
            }
        } catch (Exception e) {
            // Shutdown failed, get thread dump for debugging
            System.err.println(dumpAllThreads());

            // Log the exception, so it is visible in log file for the test class,
            // otherwise it is only visible in surefire test report
            SUPPORT_LOGGER.warning("Terminating instance factory failed", e);
            throw e;
        } finally {
            factory = null;
            instances = null;
            client = null;
        }
    }

    @Nonnull
    protected static TestHecatonFactory factory() {
        return factory;
    }

    /**
     * Returns the config used to create member instances (even if null was
     * passed).
     */
    @Nonnull
    protected static Config jetConfig() {
        return config;
    }

    /**
     * Returns the first instance.
     */
    @Nonnull
    protected static HecatonInstance instance() {
        return instances[0];
    }

    /**
     * Returns all instances (except for the client).
     */
    @Nonnull
    protected static HecatonInstance[] instances() {
        return instances;
    }

    /**
     * Returns the client or null, if a client wasn't requested.
     */
    protected static HecatonInstance client() {
        return client;
    }
}
