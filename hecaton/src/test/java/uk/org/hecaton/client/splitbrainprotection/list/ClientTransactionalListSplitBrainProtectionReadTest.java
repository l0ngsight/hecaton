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

package uk.org.hecaton.client.splitbrainprotection.list;

import uk.org.hecaton.client.splitbrainprotection.PartitionedClusterClients;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.splitbrainprotection.list.TransactionalListSplitBrainProtectionReadTest;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)

@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTransactionalListSplitBrainProtectionReadTest extends TransactionalListSplitBrainProtectionReadTest {

    private static PartitionedClusterClients clients;

    @BeforeClass
    public static void setUp() {
        TestHecatonFactory factory = new TestHecatonFactory();
        initTestEnvironment(smallInstanceConfig(), factory);
        clients = new PartitionedClusterClients(cluster, factory);
    }

    @AfterClass
    public static void tearDown() {
        if (clients != null) {
            clients.terminateAll();
        }
        clients = null;

        shutdownTestEnvironment();
    }

    @Override
    public TransactionContext newTransactionContext(int index) {
        return clients.client(index).newTransactionContext(options);
    }
}
