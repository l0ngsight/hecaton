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

import uk.org.hecaton.config.ConsistencyCheckStrategy;
import uk.org.hecaton.config.WanSyncConfig;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.json.internal.JsonSerializable;

/**
 * A JSON representation of {@link WanSyncConfig}.
 */
public class WanSyncConfigDTO implements JsonSerializable {

    private WanSyncConfig config;

    public WanSyncConfigDTO() {
    }

    public WanSyncConfigDTO(WanSyncConfig config) {
        this.config = config;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        if (config.getConsistencyCheckStrategy() != null) {
            root.add("consistencyCheckStrategy", config.getConsistencyCheckStrategy().getId());
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = new WanSyncConfig();

        JsonValue consistencyCheckStrategy = json.get("consistencyCheckStrategy");
        if (consistencyCheckStrategy != null && !consistencyCheckStrategy.isNull()) {
            config.setConsistencyCheckStrategy(ConsistencyCheckStrategy.getById(
                    (byte) consistencyCheckStrategy.asInt()));
        }
    }

    public WanSyncConfig getConfig() {
        return config;
    }
}
