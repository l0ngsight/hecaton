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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.operation.FetchPartitionStateOperation;
import uk.org.hecaton.internal.partition.operation.MigrationOperation;
import uk.org.hecaton.internal.partition.operation.MigrationRequestOperation;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.partition.IPartitionService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MasterSplitTest extends HecatonTestSupport {

    @Parameterized.Parameter
    public boolean chunkedMigrationEnabled;

    @Parameterized.Parameters(name = "chunkedMigrationEnabled:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true}, {false}
        });
    }

    private TestHecatonInstanceFactory factory;

    @Before
    public void init() {
        factory = createHecatonInstanceFactory();
    }

    @Test
    public void test_migrationFailsOnMasterMismatch_onSource() throws InterruptedException {
        HecatonInstance member1 = factory.newHecatonInstance();
        HecatonInstance member2 = factory.newHecatonInstance();

        warmUpPartitions(member1, member2);

        MigrationInfo migration = createMigrationInfo(member1, member2);

        Operation op = new MigrationRequestOperation(migration, Collections.emptyList(),
                0, true, chunkedMigrationEnabled, Integer.MAX_VALUE);

        InvocationBuilder invocationBuilder = getOperationService(member1)
                .createInvocationBuilder(SERVICE_NAME, op, getAddress(member2))
                .setCallTimeout(TimeUnit.MINUTES.toMillis(1));
        Future<?> future = invocationBuilder.invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    private MigrationInfo createMigrationInfo(HecatonInstance master, HecatonInstance nonMaster) {
        MigrationInfo migration
                = new MigrationInfo(getPartitionId(nonMaster),
                PartitionReplica.from(nonMaster.getCluster().getLocalMember()),
                PartitionReplica.from(master.getCluster().getLocalMember()),
                0, 1, -1, 0);
        migration.setMaster(getAddress(nonMaster));
        return migration;
    }

    @Test
    public void test_migrationFailsOnMasterMismatch_onDestination() throws InterruptedException {
        HecatonInstance member1 = factory.newHecatonInstance();
        HecatonInstance member2 = factory.newHecatonInstance();

        warmUpPartitions(member1, member2);

        MigrationInfo migration = createMigrationInfo(member1, member2);

        ReplicaFragmentMigrationState migrationState
                = new ReplicaFragmentMigrationState(Collections.emptyMap(),
                Collections.emptySet(), Collections.emptySet(), chunkedMigrationEnabled,
                (int) MemoryUnit.MEGABYTES.toBytes(50),
                Logger.getLogger(getClass()), 1);
        Operation op = new MigrationOperation(migration, Collections.emptyList(),
                0, migrationState, true, true);

        InvocationBuilder invocationBuilder = getOperationService(member1)
                .createInvocationBuilder(SERVICE_NAME, op, getAddress(member2))
                .setCallTimeout(TimeUnit.MINUTES.toMillis(1));
        Future<?> future = invocationBuilder.invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void test_fetchPartitionStateFailsOnMasterMismatch()
            throws InterruptedException {
        HecatonInstance member1 = factory.newHecatonInstance();
        HecatonInstance member2 = factory.newHecatonInstance();
        HecatonInstance member3 = factory.newHecatonInstance();

        warmUpPartitions(member1, member2, member3);

        InternalCompletableFuture<Object> future =
                getOperationService(member2).createInvocationBuilder(SERVICE_NAME, new FetchPartitionStateOperation(),
                                getAddress(member3))
                        .setTryCount(Integer.MAX_VALUE).setCallTimeout(Long.MAX_VALUE).invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

}
