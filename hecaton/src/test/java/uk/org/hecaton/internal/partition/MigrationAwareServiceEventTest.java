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

package uk.org.hecaton.internal.partition;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigAccessor;
import uk.org.hecaton.config.ServiceConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.spi.exception.RetryableHecatonException;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationResponseHandler;
import uk.org.hecaton.test.AssertTask;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MigrationAwareServiceEventTest extends HecatonTestSupport {

    private TestHecatonInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHecatonInstanceFactory();
    }

    @Test
    public void migrationCommitEvents_shouldBeEqual_onSource_and_onDestination() {
        Config config = new Config();
        final MigrationEventCounterService counter = new MigrationEventCounterService();
        ServiceConfig serviceConfig = new ServiceConfig()
                .setEnabled(true).setName("event-counter")
                .setImplementation(counter);
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);

        final HecatonInstance hz = factory.newHecatonInstance(config);
        warmUpPartitions(hz);

        final AssertTask assertTask = new AssertTask() {
            final InternalPartitionService partitionService = getNode(hz).getPartitionService();

            @Override
            public void run() {
                assertEquals(0, partitionService.getMigrationQueueSize());
                final int source = counter.sourceCommits.get();
                final int destination = counter.destinationCommits.get();
                assertEquals(source, destination);
            }
        };

        factory.newHecatonInstance(config);
        assertTrueEventually(assertTask);

        factory.newHecatonInstance(config);
        assertTrueEventually(assertTask);
    }

    @Test
    public void partitionIsMigratingFlag_shouldBeSet_until_serviceCommitRollback_isCompleted() {
        FailingOperationResponseHandler responseHandler = new FailingOperationResponseHandler();
        HecatonInstance hz = factory.newHecatonInstance(newConfig(responseHandler));
        warmUpPartitions(hz);

        HecatonInstance[] instances = new HecatonInstance[2];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHecatonInstance(newConfig(responseHandler));
        }
        waitAllForSafeState(instances);
        for (HecatonInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }
        waitAllForSafeState(hz);

        assertThat(responseHandler.failures).isEmpty();
    }

    private Config newConfig(FailingOperationResponseHandler responseHandler) {
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig().setEnabled(true).setImplementation(new MigrationCommitRollbackTestingService(responseHandler))
                        .setName(MigrationCommitRollbackTestingService.NAME));
        return config;
    }

    private static class FailingOperationResponseHandler implements OperationResponseHandler<Operation> {
        private final Queue<String> failures = new ConcurrentLinkedQueue<>();

        @Override
        public void sendResponse(Operation operation, Object response) {
            assert operation instanceof DummyPartitionAwareOperation : "Invalid operation: " + operation;
            NodeEngine nodeEngine = operation.getNodeEngine();
            if (!(response instanceof RetryableHecatonException) && nodeEngine.isRunning()) {
                DummyPartitionAwareOperation op = (DummyPartitionAwareOperation) operation;
                failures.add("Unexpected response: " + response + ". Node: " + nodeEngine.getThisAddress()
                        + ", Event: " + op.event + ", Type: " + op.type);
            }
        }
    }

    private static class MigrationCommitRollbackTestingService implements MigrationAwareService, ManagedService {
        private static final String NAME = MigrationCommitRollbackTestingService.class.getSimpleName();
        private static final String TYPE_COMMIT = "COMMIT";
        private static final String TYPE_ROLLBACK = "ROLLBACK";

        private final FailingOperationResponseHandler responseHandler;
        private volatile NodeEngine nodeEngine;

        MigrationCommitRollbackTestingService(FailingOperationResponseHandler responseHandler) {
            this.responseHandler = responseHandler;
        }

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            this.nodeEngine = nodeEngine;
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
            checkPartition(event, TYPE_COMMIT);
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
            checkPartition(event, TYPE_ROLLBACK);
        }

        private void checkPartition(PartitionMigrationEvent event, String type) {
            if (event.getNewReplicaIndex() != 0 && event.getCurrentReplicaIndex() != 0) {
                return;
            }

            checkPartitionMigrating(event, type);

            if (event.getCurrentReplicaIndex() != -1) {
                runPartitionOperation(event, type, event.getCurrentReplicaIndex());
            }
            if (event.getNewReplicaIndex() != -1) {
                runPartitionOperation(event, type, event.getNewReplicaIndex());
            }
        }

        private void runPartitionOperation(PartitionMigrationEvent event, String type, int replicaIndex) {
            DummyPartitionAwareOperation op = new DummyPartitionAwareOperation(event, type);
            op.setNodeEngine(nodeEngine).setPartitionId(event.getPartitionId()).setReplicaIndex(replicaIndex);
            op.setOperationResponseHandler(responseHandler);
            nodeEngine.getOperationService().run(op);
        }

        private void checkPartitionMigrating(PartitionMigrationEvent event, String type) {
            InternalPartitionServiceImpl partitionService =
                    (InternalPartitionServiceImpl) nodeEngine.getPartitionService();

            InternalPartition partition = partitionService.getPartition(event.getPartitionId());
            if (!partition.isMigrating() && nodeEngine.isRunning()) {
                responseHandler.failures.add("Migrating flag is not set. Node: " + nodeEngine.getThisAddress()
                        + ", Event: " + event + ", Type: " + type);
            }
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }
    }

    private static class DummyPartitionAwareOperation extends Operation {
        private final PartitionMigrationEvent event;
        private final String type;

        DummyPartitionAwareOperation(PartitionMigrationEvent event, String type) {
            this.event = event;
            this.type = type;
        }

        @Override
        public void run() {
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }
    }

    private static class MigrationEventCounterService implements MigrationAwareService {

        final AtomicInteger sourceCommits = new AtomicInteger();
        final AtomicInteger destinationCommits = new AtomicInteger();

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
            // Only count ownership migrations.
            // For missing (new) backups there are also COPY migrations (call it just replication)
            // which don't have a source endpoint.
            if (event.getCurrentReplicaIndex() == 0 || event.getNewReplicaIndex() == 0) {
                if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                    sourceCommits.incrementAndGet();
                } else {
                    destinationCommits.incrementAndGet();
                }
            }
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
        }

    }
}
