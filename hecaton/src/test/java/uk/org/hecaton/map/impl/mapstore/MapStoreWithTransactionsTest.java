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

package uk.org.hecaton.map.impl.mapstore;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapStoreAdapter;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.transaction.TransactionalMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static uk.org.hecaton.transaction.TransactionOptions.TransactionType.TWO_PHASE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class MapStoreWithTransactionsTest extends HecatonTestSupport {

    @Parameterized.Parameters(name = "offload: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][] {
                {true},
                {false}
        });
    }

    @Parameterized.Parameter
    public boolean offload;

    private static final String MAP_NAME = "map-with-map-store";
    private static final int KEY_COUNT = 1_001;

    @Override
    public Config getConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getMapConfig(MAP_NAME)
                .getMapStoreConfig()
                .setEnabled(true)
                .setOffload(offload)
                .setImplementation(new MapStoreAdapter());
        return config;
    }

    @Test
    public void lock_count_is_same_with_before_after_txn_commits() {
        HecatonInstance node = createHecatonInstance();

        IMap map = node.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            map.set(i, i);
        }

        for (int i = 0; i < KEY_COUNT; i++) {
            map.lock(i);
        }

        TransactionContext context = newTransactionContext(node, TWO_PHASE);
        context.beginTransaction();

        TransactionalMap txnMap = context.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            txnMap.set(i, i * 2);
        }

        context.commitTransaction();

        for (int i = 0; i < KEY_COUNT; i++) {
            assertTrue(map.isLocked(i));
        }

    }

    private static TransactionContext newTransactionContext(HecatonInstance node,
                                                            TransactionOptions.TransactionType twoPhase) {
        TransactionOptions options = new TransactionOptions()
                .setTransactionType(twoPhase);
        return node.newTransactionContext(options);
    }
}
