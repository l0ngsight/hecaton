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

import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.json.internal.JsonSerializable;

import static uk.org.hecaton.internal.util.JsonUtil.fromJsonObject;
import static uk.org.hecaton.internal.util.JsonUtil.toJsonObject;
import static uk.org.hecaton.internal.util.MapUtil.isNullOrEmpty;

/**
 * A JSON representation of {@link WanConsumerConfig}.
 */
public class WanConsumerConfigDTO implements JsonSerializable {

    private WanConsumerConfig config;

    public WanConsumerConfigDTO() {
    }

    public WanConsumerConfigDTO(WanConsumerConfig config) {
        this.config = config;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject()
                .add("persistWanReplicatedData", config.isPersistWanReplicatedData());
        if (config.getClassName() != null) {
            root.add("className", config.getClassName());
        }
        if (!isNullOrEmpty(config.getProperties())) {
            root.add("properties", toJsonObject(config.getProperties()));
        }

        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = new WanConsumerConfig();

        JsonValue persistWanReplicatedData = json.get("persistWanReplicatedData");
        if (persistWanReplicatedData != null && !persistWanReplicatedData.isNull()) {
            config.setPersistWanReplicatedData(persistWanReplicatedData.asBoolean());
        }

        JsonValue className = json.get("className");
        if (className != null && !className.isNull()) {
            config.setClassName(className.asString());
        }

        config.setProperties(fromJsonObject((JsonObject) json.get("properties")));
    }

    public WanConsumerConfig getConfig() {
        return config;
    }


}
