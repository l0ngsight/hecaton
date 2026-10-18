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

package uk.org.hecaton.map.impl.querycache.event;

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleIMapEventTest {

    private QueryCacheEventData queryCacheEventData;
    private SingleIMapEvent singleIMapEvent;

    @Before
    public void setUp() {
        queryCacheEventData = new DefaultQueryCacheEventData();

        singleIMapEvent = new SingleIMapEvent(queryCacheEventData);
    }

    @Test
    public void testGetEventData() {
        assertEquals(queryCacheEventData, singleIMapEvent.getEventData());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMember() {
        singleIMapEvent.getMember();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetEventType() {
        singleIMapEvent.getEventType();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetName() {
        singleIMapEvent.getName();
    }
}
