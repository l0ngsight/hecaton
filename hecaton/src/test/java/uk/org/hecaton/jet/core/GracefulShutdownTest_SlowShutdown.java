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
import uk.org.hecaton.config.ConfigAccessor;
import uk.org.hecaton.config.ServiceConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.services.GracefulShutdownAwareService;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.core.processor.DiagnosticProcessors;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.starter.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.jet.core.Edge.between;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({QuickTest.class, ParallelJVMTest.class})
public class GracefulShutdownTest_SlowShutdown extends JetTestSupport {

    private static final int NODE_COUNT = 3;

    private HecatonInstance[] instances;

    private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private final CountDownLatch shutdownProceed = new CountDownLatch(1);

    /**
     * Blocks first shutdown until unblocked. Subsequent shutdowns run normally.
     */
    private final GracefulShutdownAwareService slowShutdownService = (timeout, unit) -> {
        shutdownInitiated.countDown();
        try {
            shutdownProceed.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return true;
    };

    @Before
    public void setup() {
        // The setup of instances is as follows:
        // #0 - master
        // #1 - will be shut down slowly (simulating e.g. long-running migrations)
        // #2 - non-master member which will be receiving job requests
        instances = createHecatonInstances(newConfigWithSlowShutdownService(), NODE_COUNT);
    }

    private Config newConfigWithSlowShutdownService() {
        Config config = smallInstanceConfig();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(new ServiceConfig()
                .setEnabled(true)
                .setName("slow-shutdown-service")
                .setImplementation(slowShutdownService));
        return config;
    }

    @After
    public void unblockShutdown() {
        // especially useful in case of assertion failures
        shutdownProceed.countDown();
    }

    @Test
    public void when_shutdownWithRaceCondition_then_failJobSubmittedToMaster() throws IllegalAccessException {
        when_shutdownWithRaceCondition_then_failJob(instances[0]);
    }

    @Test
    public void when_shutdownWithRaceCondition_then_failJobSubmittedToNonMaster() throws IllegalAccessException {
        when_shutdownWithRaceCondition_then_failJob(instances[2]);
    }

    private void when_shutdownWithRaceCondition_then_failJob(HecatonInstance submitTo) throws IllegalAccessException {
        // Given
        DAG dag = getDag();

        Node node = Accessors.getNode(instances[1]);
        // some hackery to simulate scenario when member is shutting down
        // but the notification has not yet arrived to master/other members
        AtomicBoolean shuttingDown = ReflectionUtils.getFieldValueReflectively(node, "shuttingDown");
        try {
            shuttingDown.set(true);
            // When
            Job job = submit(dag, submitTo);

            // Then
            assertThatThrownBy(job::join)
                    .hasRootCauseInstanceOf(TopologyChangedException.class);
        } finally {
            // allow normal shutdown during test tear down
            shuttingDown.set(false);
        }
    }

    @Test
    public void when_slowShutdown_thenNoException() throws InterruptedException {
        DAG dag = getDag();

        Executors.newSingleThreadExecutor().submit(() -> instances[1].shutdown());
        shutdownInitiated.await();

        var shutdownedMemberUUID = instances[1].getCluster().getLocalMember().getUuid();

        assertShuttingDownMembers(emptySet(), instances[1]);
        assertTrueEventually(() -> assertShuttingDownMembers(singleton(shutdownedMemberUUID), instances[0]));
        assertTrueEventually(() -> assertShuttingDownMembers(singleton(shutdownedMemberUUID), instances[2]));

        Job job = submit(dag);
        assertThatNoException().isThrownBy(job::join);
    }

    @Test
    public void when_shutDown_allMembersAddMemberShuttingDownListAndThenRemove() throws InterruptedException {
        Executors.newSingleThreadExecutor().submit(() -> instances[1].shutdown());
        shutdownInitiated.await();

        var shutdownedMemberUUID = instances[1].getCluster().getLocalMember().getUuid();

        assertShuttingDownMembers(emptySet(), instances[1]);
        assertTrueEventually(() -> assertShuttingDownMembers(singleton(shutdownedMemberUUID), instances[0]));
        assertTrueEventually(() -> assertShuttingDownMembers(singleton(shutdownedMemberUUID), instances[2]));

        shutdownProceed.countDown();

        assertTrueEventually(() -> assertShuttingDownMembers(emptySet(), instances[0]));
        assertTrueEventually(() -> assertShuttingDownMembers(emptySet(), instances[2]));
    }

    @Nonnull
    private static DAG getDag() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", TestProcessors.ListSource.supplier(singletonList(1)));
        Vertex process = dag.newVertex("faulty", DiagnosticProcessors.writeLoggerP());
        dag.edge(between(source, process));
        return dag;
    }

    private Job submit(DAG dag) {
        return submit(dag, instances[2]);
    }

    private Job submit(DAG dag, HecatonInstance instance) {
        return instance.getJet().newLightJob(dag);
    }

    private void assertShuttingDownMembers(Set<UUID> expected, HecatonInstance instance) {
        JetServiceBackend service = Accessors.getService(instance, JetServiceBackend.SERVICE_NAME);
        var jobCoordinatorService = service.getJobCoordinationService();
        Map<UUID, CompletableFuture<Void>> membersShuttingDown;
        try {
            membersShuttingDown = ReflectionUtils.getFieldValueReflectively(jobCoordinatorService, "membersShuttingDown");
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        assertThat(membersShuttingDown.keySet()).isEqualTo(expected);
    }
}
