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

package uk.org.hecaton.collection.impl.list;

import uk.org.hecaton.collection.impl.AbstractCollectionBackupTest;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static uk.org.hecaton.collection.impl.CollectionTestUtil.getBackupList;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListBackupTest extends AbstractCollectionBackupTest {

    @Test
    public void testBackupPromotion() {
        config.getListConfig("default")
                .setBackupCount(1)
                .setAsyncBackupCount(0);

        testBackupPromotionInternal();
    }

    @Test
    public void testBackupMigration() {
        config.getListConfig("default")
                .setBackupCount(BACKUP_COUNT)
                .setAsyncBackupCount(0);

        testBackupMigrationInternal();
    }

    @Test
    public void testAsyncBackupCount() {
        config.getListConfig("testAsyncBackupCount")
              .setBackupCount(1)
              .setAsyncBackupCount(4);
    }

    @Override
    protected Collection<Integer> getHecatonCollection(HecatonInstance instance, String name) {
        return instance.getList(name);
    }

    @Override
    protected Collection<Integer> getBackupCollection(HecatonInstance instance, String name) {
        return getBackupList(instance, name);
    }
}
