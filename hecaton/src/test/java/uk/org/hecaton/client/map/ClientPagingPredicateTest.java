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

package uk.org.hecaton.client.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.PagingPredicateTest;
import uk.org.hecaton.query.PagingPredicate;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Used for testing {@link PagingPredicate}
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPagingPredicateTest extends PagingPredicateTest {

    @Override
    protected HecatonInstance createRemote(Config config) {
        return hecatonFactory.newHecatonClient();
    }

    @Test
    @Ignore("local paging is not supported on clients")
    @Override
    public void testLocalPaging() {
        // do nothing
    }

    @Test
    @Ignore("local entry listener is not supported on clients")
    @Override
    public void testAddLocalEntryListenerThrowsWithPagingPredicate() {
        // do nothing
    }

    @Test
    @Ignore("local entry listener is not supported on clients")
    @Override
    public void testAddLocalEntryListenerThrowsWithPredicateIncludingPagingPredicate() {
        // do nothing
    }

    @Test
    @Ignore("local entry listener is not supported on clients")
    @Override
    public void testAddLocalEntryListenerWithKeyThrowsWithPagingPredicate() {
        // do nothing
    }

    @Test
    @Ignore("local entry listener is not supported on clients")
    @Override
    public void testAddLocalEntryListenerWithKeyThrowsWithPredicateIncludingPagingPredicate() {
    }

}
