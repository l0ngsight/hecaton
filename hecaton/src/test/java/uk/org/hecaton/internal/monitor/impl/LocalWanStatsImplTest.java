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

package uk.org.hecaton.internal.monitor.impl;

import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.monitor.LocalWanPublisherStats;
import uk.org.hecaton.internal.monitor.LocalWanStats;
import uk.org.hecaton.json.internal.JsonSerializable;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.WanPublisherState;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalWanStatsImplTest {

    @Test
    public void testSerialization() {
        LocalWanPublisherStatsImpl tokyo = new LocalWanPublisherStatsImpl();
        tokyo.setConnected(true);
        tokyo.incrementPublishedEventCount(10);
        tokyo.setOutboundQueueSize(100);
        tokyo.setState(WanPublisherState.REPLICATING);

        LocalWanPublisherStatsImpl singapore = new LocalWanPublisherStatsImpl();
        singapore.setConnected(true);
        singapore.setOutboundQueueSize(200);
        singapore.incrementPublishedEventCount(20);
        singapore.setState(WanPublisherState.REPLICATING);

        LocalWanStatsImpl localWanStats = new LocalWanStatsImpl();
        Map<String, LocalWanPublisherStats> localWanPublisherStatsMap = new HashMap<>();
        localWanPublisherStatsMap.put("tokyo", tokyo);
        localWanPublisherStatsMap.put("singapore", singapore);
        localWanStats.setLocalPublisherStatsMap(localWanPublisherStatsMap);

        JsonObject serialized = localWanStats.toJson();

        LocalWanStats deserialized = new LocalWanStatsImpl();
        ((JsonSerializable) deserialized).fromJson(serialized);

        LocalWanPublisherStats deserializedTokyo = deserialized.getLocalWanPublisherStats().get("tokyo");
        LocalWanPublisherStats deserializedSingapore = deserialized.getLocalWanPublisherStats().get("singapore");

        assertEquals(tokyo.isConnected(), deserializedTokyo.isConnected());
        assertEquals(tokyo.getPublisherState(), deserializedTokyo.getPublisherState());

        assertEquals(singapore.isConnected(), deserializedSingapore.isConnected());
        assertEquals(singapore.getPublisherState(), deserializedSingapore.getPublisherState());
    }

}
