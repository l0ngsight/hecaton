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

package uk.org.hecaton.internal.management.dto;

import uk.org.hecaton.internal.json.JsonArray;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.json.internal.JsonSerializable;

import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.internal.util.JsonUtil.getArray;
import static uk.org.hecaton.internal.util.JsonUtil.getInt;
import static uk.org.hecaton.internal.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link uk.org.hecaton.spi.impl.operationexecutor.slowoperationdetector.SlowOperationLog}.
 */
@SuppressWarnings("VisibilityModifier")
public class SlowOperationDTO implements JsonSerializable {

    public String operation;
    public String stackTrace;
    public int totalInvocations;
    public List<SlowOperationInvocationDTO> invocations;

    public SlowOperationDTO() {
    }

    public SlowOperationDTO(String operation, String stackTrace, int totalInvocations,
                            List<SlowOperationInvocationDTO> invocations) {
        this.operation = operation;
        this.stackTrace = stackTrace;
        this.totalInvocations = totalInvocations;
        this.invocations = invocations;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("operation", operation);
        root.add("stackTrace", stackTrace);
        root.add("totalInvocations", totalInvocations);
        JsonArray invocationArray = new JsonArray();
        for (SlowOperationInvocationDTO invocation : invocations) {
            JsonObject json = invocation.toJson();
            if (json != null) {
                invocationArray.add(json);
            }
        }
        root.add("invocations", invocationArray);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        operation = getString(json, "operation");
        stackTrace = getString(json, "stackTrace");
        totalInvocations = getInt(json, "totalInvocations");

        invocations = new ArrayList<>();
        for (JsonValue jsonValue : getArray(json, "invocations")) {
            SlowOperationInvocationDTO slowOperationInvocationDTO = new SlowOperationInvocationDTO();
            slowOperationInvocationDTO.fromJson(jsonValue.asObject());
            invocations.add(slowOperationInvocationDTO);
        }
    }
}
