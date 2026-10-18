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

package uk.org.hecaton.listeners;

import uk.org.hecaton.core.EntryListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AddListenerWithNullParameterTests extends HecatonTestSupport {

    protected HecatonInstance instance;

    @Before
    public void setup() {
        instance = createHecatonInstance();
    }

    @Test(expected = NullPointerException.class)
    public void testMapAddEntryListener() {
        instance.getMap("test").addEntryListener(null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testMapAddEntryListenerKey() {
        instance.getMap("test").addEntryListener(null, "key", true);
    }

    @Test(expected = NullPointerException.class)
    public void testMapAddEntryListenerPredicate() {
        instance.getMap("test").addEntryListener(null, Predicates.alwaysTrue(), true);
    }

    @Test(expected = NullPointerException.class)
    public void testMapAddEntryListener_NullPredicate() {
        instance.getMap("test").addEntryListener(mock(EntryListener.class), null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testMapAddEntryListenerPredicateAndKey() {
        instance.getMap("test").addEntryListener(null, Predicates.alwaysTrue(), "key", true);
    }

    @Test(expected = NullPointerException.class)
    public void testMapAddEntryListenerKeyAnd_NullPredicate() {
        instance.getMap("test").addEntryListener(mock(EntryListener.class), null, "key", true);
    }

    @Test(expected = NullPointerException.class)
    public void testMultiMapAddEntryListener() {
        instance.getMultiMap("test").addEntryListener(null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testMultiMapAddEntryListenerKey() {
        instance.getMultiMap("test").addEntryListener(null, "key", true);
    }

    @Test(expected = NullPointerException.class)
    public void testReplicatedMapAddEntryListener() {
        instance.getReplicatedMap("test").addEntryListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testReplicatedMapAddEntryListenerKey() {
        instance.getReplicatedMap("test").addEntryListener(null, "key");
    }

    @Test(expected = NullPointerException.class)
    public void testReplicatedMapAddEntryListenerPredicate() {
        instance.getReplicatedMap("test").addEntryListener(null, Predicates.alwaysTrue());
    }

    @Test(expected = NullPointerException.class)
    public void testReplicatedMapAddEntryListener_NullPredicate() {
        instance.getReplicatedMap("test").addEntryListener(mock(EntryListener.class), null);
    }

    @Test(expected = NullPointerException.class)
    public void testReplicatedMapAddEntryListenerPredicateAndKey() {
        instance.getReplicatedMap("test").addEntryListener(null, Predicates.alwaysTrue(), "key");
    }

    @Test(expected = NullPointerException.class)
    public void testReplicatedMapAddEntryListenerKeyAnd_NullPredicate() {
        instance.getReplicatedMap("test").addEntryListener(mock(EntryListener.class), null, "key");
    }

    @Test(expected = NullPointerException.class)
    public void testListAddItemListener() {
        instance.getList("test").addItemListener(null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testSetAddItemListener() {
        instance.getSet("test").addItemListener(null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testQueueAddItemListener() {
        instance.getQueue("test").addItemListener(null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testTopicAddMessageListener() {
        instance.getTopic("test").addMessageListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testReliableTopicAddMessageListener() {
        instance.getReliableTopic("test").addMessageListener(null);
    }
}
