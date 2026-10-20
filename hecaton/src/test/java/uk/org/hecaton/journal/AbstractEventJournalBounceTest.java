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

package uk.org.hecaton.journal;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.journal.EventJournalInitialSubscriberState;
import uk.org.hecaton.internal.journal.EventJournalReader;
import uk.org.hecaton.ringbuffer.ReadResultSet;
import uk.org.hecaton.test.bounce.BounceMemberRule;
import uk.org.hecaton.test.bounce.BounceTestConfiguration;
import uk.org.hecaton.test.jitter.JitterRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedList;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.spi.properties.ClusterProperty.EVENT_THREAD_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.GENERIC_OPERATION_THREAD_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

/**
 * Base class for an bouncing event journal read test. The test will fill
 * the data structure after which it will start gracefully shutting down
 * instances and starting new instances while concurrently reading from
 * the event journal.
 */
public abstract class AbstractEventJournalBounceTest {

    private static final int TEST_PARTITION_COUNT = 10;
    private static final int CONCURRENCY = 10;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(4)
            .driverType(BounceTestConfiguration.DriverType.MEMBER)
            .build();

    @Rule
    public JitterRule jitterRule = new JitterRule();
    private LinkedList<Object> expectedEvents;

    @Before
    public void setup() {
        final HecatonInstance instance = bounceMemberRule.getSteadyMember();
        fillDataStructure(instance);
        expectedEvents = getEventJournalEvents(getEventJournalReader(instance));
    }

    @Test
    public void testBouncingEventJournal() {
        EventJournalReadRunnable[] testTasks = new EventJournalReadRunnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            testTasks[i] = new EventJournalReadRunnable(bounceMemberRule.getNextTestDriver(), expectedEvents);
        }
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(3));
    }

    protected abstract void fillDataStructure(HecatonInstance instance);

    protected abstract <T> EventJournalReader<T> getEventJournalReader(HecatonInstance instance);

    protected Config getConfig() {
        return new Config()
                .setProperty(PARTITION_COUNT.getName(), String.valueOf(TEST_PARTITION_COUNT))
                .setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "4")
                .setProperty(GENERIC_OPERATION_THREAD_COUNT.getName(), "4")
                .setProperty(EVENT_THREAD_COUNT.getName(), "1");
    }

    class EventJournalReadRunnable<T> implements Runnable {
        private final HecatonInstance hecatonInstance;
        private final LinkedList<T> expected;
        private EventJournalReader<T> reader;

        EventJournalReadRunnable(HecatonInstance hecatonInstance, LinkedList<T> expected) {
            this.hecatonInstance = hecatonInstance;
            this.expected = expected;
        }

        @Override
        public void run() {
            if (reader == null) {
                reader = getEventJournalReader(hecatonInstance);
            }
            final LinkedList<T> actual = getEventJournalEvents(reader);
            assertEquals(expected, actual);
        }
    }

    private <T> LinkedList<T> getEventJournalEvents(EventJournalReader<T> reader) {
        final LinkedList<T> events = new LinkedList<>();

        for (int i = 1; i < TEST_PARTITION_COUNT; i++) {
            try {
                final EventJournalInitialSubscriberState state = reader.subscribeToEventJournal(i).toCompletableFuture().get();
                final ReadResultSet<T> partitionEvents = reader.readFromEventJournal(
                        state.getOldestSequence(), 1,
                        (int) (state.getNewestSequence() - state.getOldestSequence() + 1), i,
                        new TruePredicate<>(), new IdentityFunction<>()).toCompletableFuture().get();
                for (T event : partitionEvents) {
                    events.add(event);
                }
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
        return events;
    }
}
