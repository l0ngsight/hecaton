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

package uk.org.hecaton.splitbrainprotection.multimap;

import uk.org.hecaton.splitbrainprotection.AbstractSplitBrainProtectionTest;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.transaction.TransactionalMultiMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn.READ;
import static uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static uk.org.hecaton.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static uk.org.hecaton.transaction.TransactionOptions.TransactionType.TWO_PHASE;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionalMultiMapSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "Executing: {0} {1}")
    public static Collection<Object[]> parameters() {

        TransactionOptions onePhaseOption = TransactionOptions.getDefault();
        onePhaseOption.setTransactionType(ONE_PHASE);

        TransactionOptions twoPhaseOption = TransactionOptions.getDefault();
        twoPhaseOption.setTransactionType(TWO_PHASE);

        return Arrays.asList(
                new Object[]{onePhaseOption, READ},
                new Object[]{twoPhaseOption, READ},
                new Object[]{onePhaseOption, READ_WRITE},
                new Object[]{twoPhaseOption, READ_WRITE}
        );
    }

    @Parameter(0)
    public static TransactionOptions options;

    @Parameter(1)
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHecatonInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void txSize_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMultiMap<Object, Object> map = transactionContext.getMultiMap(MULTI_MAP_NAME + splitBrainProtectionOn.name());
        map.size();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txSize_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMultiMap<Object, Object> map = transactionContext.getMultiMap(MULTI_MAP_NAME + splitBrainProtectionOn.name());
        map.size();
        transactionContext.commitTransaction();
    }

    @Test
    public void txGet_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMultiMap<Object, Object> map = transactionContext.getMultiMap(MULTI_MAP_NAME + splitBrainProtectionOn.name());
        map.get("123");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txGet_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMultiMap<Object, Object> map = transactionContext.getMultiMap(MULTI_MAP_NAME + splitBrainProtectionOn.name());
        map.get("123");
        transactionContext.commitTransaction();
    }

    @Test
    public void txValuesCount_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMultiMap<Object, Object> map = transactionContext.getMultiMap(MULTI_MAP_NAME + splitBrainProtectionOn.name());
        map.valueCount("123");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txValuesCount_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMultiMap<Object, Object> map = transactionContext.getMultiMap(MULTI_MAP_NAME + splitBrainProtectionOn.name());
        map.valueCount("123");
        transactionContext.commitTransaction();
    }

    public TransactionContext newTransactionContext(int index) {
        return cluster.instance[index].newTransactionContext(options);
    }
}
