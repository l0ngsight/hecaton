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

import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.json.JsonArray;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.internal.management.dto.SlowOperationDTO;
import uk.org.hecaton.internal.monitor.LocalOperationStats;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.json.internal.JsonSerializable;

import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.internal.util.JsonUtil.getArray;
import static uk.org.hecaton.internal.util.JsonUtil.getLong;
import static uk.org.hecaton.spi.properties.ClusterProperty.MC_MAX_VISIBLE_SLOW_OPERATION_COUNT;

/**
 * Hecaton statistic implementations for local operations.
 */
public class LocalOperationStatsImpl implements LocalOperationStats, JsonSerializable {

    private long maxVisibleSlowOperationCount;
    private final List<SlowOperationDTO> slowOperations;
    private long creationTime;

    public LocalOperationStatsImpl() {
        this.maxVisibleSlowOperationCount = Long.MAX_VALUE;
        this.slowOperations = new ArrayList<>();
        this.creationTime = Clock.currentTimeMillis();
    }

    public LocalOperationStatsImpl(Node node) {
        this.maxVisibleSlowOperationCount = node.getProperties().getInteger(MC_MAX_VISIBLE_SLOW_OPERATION_COUNT);
        this.slowOperations = node.nodeEngine.getOperationService().getSlowOperationDTOs();
        this.creationTime = Clock.currentTimeMillis();
    }

    public long getMaxVisibleSlowOperationCount() {
        return maxVisibleSlowOperationCount;
    }

    public List<SlowOperationDTO> getSlowOperations() {
        return slowOperations;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("maxVisibleSlowOperationCount", maxVisibleSlowOperationCount);
        JsonArray slowOperationArray = new JsonArray();
        int logCount = 0;
        for (SlowOperationDTO slowOperation : slowOperations) {
            if (logCount++ < maxVisibleSlowOperationCount) {
                slowOperationArray.add(slowOperation.toJson());
            }
        }
        root.add("slowOperations", slowOperationArray);
        root.add("creationTime", creationTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        maxVisibleSlowOperationCount = getLong(json, "maxVisibleSlowOperationCount", Long.MAX_VALUE);
        for (JsonValue jsonValue : getArray(json, "slowOperations")) {
            SlowOperationDTO slowOperationDTO = new SlowOperationDTO();
            slowOperationDTO.fromJson(jsonValue.asObject());
            slowOperations.add(slowOperationDTO);
        }
        creationTime = getLong(json, "creationTime", -1L);
    }

    @Override
    public String toString() {
        return "LocalOperationStatsImpl{"
                + "maxVisibleSlowOperationCount=" + maxVisibleSlowOperationCount
                + ", slowOperations=" + slowOperations
                + ", creationTime=" + creationTime
                + '}';
    }
}
