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

package uk.org.hecaton.map.impl.tx;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.transaction.TransactionalMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapTransactionLockingTest extends HecatonTestSupport {

    String mapName;
    String key;
    String value;

    @Before
    public void setUp() throws Exception {
        mapName = randomMapName();
        key = randomString();
        value = randomString();
    }

    @Test
    public void testTxnReplace_whenReplaceIfSameFails_keyShouldRemainUnlocked() {
        HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        IMap<String, Object> map = hecatonInstance.getMap(mapName);
        map.put(key, value);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.replace(key, value + "other", value);
            return null;
        });
        assertFalse("Key remains locked!", map.isLocked(key));
    }

    @Test
    public void testTxnReplace_whenReplaceIfSameFails_keyShouldRemainUnlockedDuringTransaction() {
        final HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        final IMap<String, Object> map = hecatonInstance.getMap(mapName);
        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            boolean replace = transactionalMap.replace(key, value + "other", value);
            assertFalse(replace);
            assertFalse("Key remains locked!", map.isLocked(key));
            return null;
        });

    }

    @Test
    public void testTxnReplace_whenReplaceIfSameFails_keyShouldRemainLocked_whenExplicitlyLocked() {
        final HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        final IMap<String, Object> map = hecatonInstance.getMap(mapName);
        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.getForUpdate(key);
            boolean replace = transactionalMap.replace(key, value + "other", value);
            assertFalse(replace);
            assertTrue("Key remains unlocked!", map.isLocked(key));
            return null;
        });
    }

    @Test
    public void testTxnReplace_whenReplaceFails_keyShouldRemainUnlocked() {
        HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        IMap<String, Object> map = hecatonInstance.getMap(mapName);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.replace(key, value);
            return null;
        });
        assertFalse("Key remains locked!", map.isLocked(key));
    }

    @Test
    public void testTxnReplace_whenReplaceFails_keyShouldRemainUnlockedDuringTransaction() {
        final HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        final IMap<String, Object> map = hecatonInstance.getMap(mapName);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.replace(key, value);
            assertFalse("Key remains locked!", map.isLocked(key));
            return null;
        });
    }

    @Test
    public void testTxnReplace_whenReplaceFails_keyShouldRemainLocked_whenExplicitlyLocked() {
        final HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        final IMap<String, Object> map = hecatonInstance.getMap(mapName);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.getForUpdate(key);
            transactionalMap.replace(key, value);
            assertTrue("Key remains unlocked!", map.isLocked(key));
            return null;
        });
    }

    @Test
    public void testTxnPutIfAbsent_whenPutFails_keyShouldRemainUnlocked() {
        HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        IMap<String, Object> map = hecatonInstance.getMap(mapName);
        map.put(key, value);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.putIfAbsent(key, "t");
            return null;
        });
        assertFalse("Key remains locked!", map.isLocked(key));
    }

    @Test
    public void testTxnPutIfAbsent_whenPutFails_keyShouldRemainUnlockedDuringTransaction() {
        final HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        final IMap<String, Object> map = hecatonInstance.getMap(mapName);
        map.put(key, value);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.putIfAbsent(key, "t");
            assertFalse("Key remains locked!", map.isLocked(key));
            return null;
        });
    }

    @Test
    public void testTxnPutIfAbsent_whenPutFails_keyShouldRemainLocked_whenExplicitlyLocked() {
        final HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        final IMap<String, Object> map = hecatonInstance.getMap(mapName);
        map.put(key, value);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.getForUpdate(key);
            transactionalMap.putIfAbsent(key, "t");
            assertTrue("Key remains unlocked!", map.isLocked(key));
            return null;
        });
    }

    @Test
    public void testTxnRemoveIfSame_whenRemoveFails_keyShouldRemainUnlocked() {
        HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        IMap<String, Object> map = hecatonInstance.getMap(mapName);
        map.put(key, value);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.remove(key, value + "other");
            return null;
        });
        assertFalse("Key remains locked!", map.isLocked(key));
    }


    @Test
    public void testTxnRemoveIfSame_whenRemoveFails_keyShouldRemainUnlockedDuringTransaction() {
        final HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        final IMap<String, Object> map = hecatonInstance.getMap(mapName);
        map.put(key, value);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.remove(key, "");
            assertFalse("Key remains locked!", map.isLocked(key));
            return null;
        });
    }

    @Test
    public void testTxnRemoveIfSame_whenRemoveFails_keyShouldRemainLocked_whenExplicitlyLocked() {
        final HecatonInstance hecatonInstance = createHecatonInstance(getConfig());
        final IMap<String, Object> map = hecatonInstance.getMap(mapName);
        map.put(key, value);

        hecatonInstance.executeTransaction(transactionContext -> {
            TransactionalMap<String, Object> transactionalMap = transactionContext.getMap(mapName);
            transactionalMap.getForUpdate(key);
            transactionalMap.remove(key, "");
            assertTrue("Key remains unlocked!", map.isLocked(key));
            return null;
        });
    }

}
