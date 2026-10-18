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

package uk.org.hecaton.internal.management;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.ParseException;
import uk.org.hecaton.internal.management.dto.MCEventDTO;
import uk.org.hecaton.internal.management.events.Event;
import uk.org.hecaton.internal.management.events.EventMetadata;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static uk.org.hecaton.internal.management.MCEventStoreTest.MC_1_UUID;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmpty;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManagementCenterServiceIntegrationTest
        extends HecatonTestSupport {

    private static final String CLUSTER_NAME = "mc-service-tests";

    private TestHecatonFactory factory;
    private HecatonInstance instance;
    private ManagementCenterService mcs;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory(1);
        instance = factory.newHecatonInstance(getConfig().setClusterName(CLUSTER_NAME));

        assertTrueEventually(() -> {
            ManagementCenterService mcs = getNode(instance).getManagementCenterService();
            assertNotNull(mcs);
            this.mcs = mcs;
            mcs.clear();
        });
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testTimedMemberState_returnsNotNull() {
        String responseString = mcs.getTimedMemberStateJson().orElse(null);
        assertFalse(isNullOrEmpty(responseString));

        JsonObject object;
        try {
            object = Json.parse(responseString).asObject();
        } catch (ParseException e) {
            throw new AssertionError("Failed to parse JSON: " + responseString);
        }
        TimedMemberState memberState = new TimedMemberState();
        memberState.fromJson(object.get("timedMemberState").asObject());
        assertEquals(CLUSTER_NAME, memberState.getClusterName());
    }

    @Test
    public void testTimedMemberState_usesCache_shortTimeFrame() {
        assertTrueEventually(() -> {
            String responseOne = mcs.getTimedMemberStateJson().orElse(null);
            String responseTwo = mcs.getTimedMemberStateJson().orElse(null);
            assertSame(responseOne, responseTwo);
        });
    }

    @Test
    public void testTimedMemberState_ignoresCache_longTimeFrame() {
        String responseOne = mcs.getTimedMemberStateJson().orElse(null);
        sleepSeconds(2);
        String responseTwo = mcs.getTimedMemberStateJson().orElse(null);
        assertNotSame(responseOne, responseTwo);
    }

    @Test
    public void testMCEvents_storesEvents_recentPoll() {
        TestEvent expectedEvent = new TestEvent();
        mcs.log(expectedEvent);
        mcs.log(new TestEvent());
        mcs.log(new TestEvent());

        List<MCEventDTO> actualEvents = mcs.pollMCEvents(MC_1_UUID);
        assertEquals(3, actualEvents.size());
        assertEquals(MCEventDTO.fromEvent(expectedEvent), actualEvents.get(0));
    }

    @Test
    public void testMCEvents_storesEvents_noPollAtAll() {
        mcs.log(new TestEvent());
        mcs.log(new TestEvent());

        assertEquals(2, mcs.pollMCEvents(MC_1_UUID).size());
    }

    @Test
    public void testMCEvents_clearsEventQueue_noRecentPoll() {
        mcs.log(new TestEvent());
        mcs.log(new TestEvent());

        mcs.onMCEventWindowExceeded();
        assertEquals(0, mcs.pollMCEvents(MC_1_UUID).size());

        mcs.log(new TestEvent(System.currentTimeMillis()));
        assertEquals(1, mcs.pollMCEvents(MC_1_UUID).size());
    }

    static class TestEvent
            implements Event {

        private final long timestamp;

        TestEvent() {
            this(42);
        }

        TestEvent(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public EventMetadata.EventType getType() {
            return EventMetadata.EventType.WAN_SYNC_STARTED;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public JsonObject toJson() {
            return new JsonObject();
        }

    }

}
