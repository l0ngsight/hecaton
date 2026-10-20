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
package uk.org.hecaton.jet.pipeline;

import uk.org.hecaton.cache.CacheEventType;
import uk.org.hecaton.cache.EventJournalCacheEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.PredicateEx;
import org.junit.Before;
import org.junit.Test;

import static uk.org.hecaton.jet.Util.cachePutEvents;
import static uk.org.hecaton.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

public class CacheJournalLostEventTest extends AbstractJournalLostEventTest {

    private String sourceCache;

    @Before
    public void before() {
        sourceCache = journaledCacheName();
    }

    @Test
    public void defaultProjection1_receivedEvent() {
        performTest(
                member,
                Sources.cacheJournalEntries(sourceCache, START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents,
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void customProjection_allEvents_receivedEvent() {
        performTest(
                member,
                Sources.cacheJournal(sourceCache, START_FROM_OLDEST, EventJournalCacheEvent::isAfterLostEvents, PredicateEx.alwaysTrue()),
                FunctionEx.identity(),
                EventFilterType.ALL
        );
    }

    @Test
    public void customProjection_putEvents_receivedEvent() {
        performTest(
                member,
                Sources.cacheJournal(sourceCache, START_FROM_OLDEST, EventJournalCacheEvent::isAfterLostEvents, cachePutEvents()),
                FunctionEx.identity(),
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void customProjection_removeEvent_receivedEvent() {
        performTest(
                member,
                Sources.cacheJournal(sourceCache, START_FROM_OLDEST, EventJournalCacheEvent::isAfterLostEvents, e -> e.getType() == CacheEventType.REMOVED),
                FunctionEx.identity(),
                EventFilterType.ONLY_REMOVE
        );
    }

    @Test
    public void remoteSourceCustomProjection_receivedEvent() {
        performTest(
                remoteInstance,
                Sources.remoteCacheJournal(sourceCache, remoteHzClientConfig, START_FROM_OLDEST, EventJournalCacheEvent::isAfterLostEvents, cachePutEvents()),
                FunctionEx.identity(),
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void remoteSourceDefaultProjection_receivedEvent() {
        performTest(
                remoteInstance,
                Sources.remoteCacheJournalEntries(sourceCache, remoteHzClientConfig, START_FROM_OLDEST),
                JournalSourceEntry::isAfterLostEvents,
                EventFilterType.ONLY_PUT
        );
    }

    @Test
    public void performRareEventTest() {
        var streamSource = Sources.cacheJournal(
                sourceCache,
                START_FROM_OLDEST,
                EventJournalCacheEvent::isAfterLostEvents,
                e -> e.getType() == CacheEventType.REMOVED
        );
        performRareEventTest(streamSource);
    }

    @Override
    protected void put(HecatonInstance hz, Integer key, Integer value) {
        hz.getCacheManager().getCache(sourceCache).put(key, value);
    }

    @Override
    protected void remove(HecatonInstance hz, Integer key) {
        hz.getCacheManager().getCache(sourceCache).remove(key);
    }
}
