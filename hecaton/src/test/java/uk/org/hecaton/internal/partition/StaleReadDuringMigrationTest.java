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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.impl.InternalPartitionImpl;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.spi.exception.PartitionMigratingException;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.test.Accessors.getOperationService;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StaleReadDuringMigrationTest extends HecatonTestSupport {

    @Test
    public void testReadOperationFailsWhenStaleReadDisabledDuringMigration()
            throws InterruptedException {
        final Config config = new Config();
        config.setProperty(ClusterProperty.DISABLE_STALE_READ_ON_PARTITION_MIGRATION.getName(), "true");

        final InternalCompletableFuture<Boolean> future = invokeOperation(config);
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PartitionMigratingException);
        }
    }

    @Test
    public void testReadOperationSucceedsWhenStaleReadEnabledDuringMigration()
            throws ExecutionException, InterruptedException {
        final InternalCompletableFuture<Boolean> future = invokeOperation(new Config());

        assertTrue(future.get());
    }

    private InternalCompletableFuture<Boolean> invokeOperation(final Config config) {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance instance = factory.newHecatonInstance(config);
        warmUpPartitions(instance);

        final int partitionId = 0;
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);
        partition.setMigrating();

        final OperationServiceImpl operationService = getOperationService(instance);
        final InvocationBuilder invocationBuilder = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new DummyOperation(), partitionId);
        return invocationBuilder.invoke();
    }

    private static class DummyOperation extends Operation implements ReadonlyOperation {

        @Override
        public void run() {
        }

        @Override
        public Object getResponse() {
            return true;
        }

        @Override
        public ExceptionAction onInvocationException(Throwable throwable) {
            if (throwable instanceof PartitionMigratingException) {
                return ExceptionAction.THROW_EXCEPTION;
            }
            return super.onInvocationException(throwable);
        }
    }
}
