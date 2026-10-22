/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.wan.custom;

import uk.org.hecaton.config.WanConsumerConfig;

/**
 * The WAN consumer the full Spring configurations name: members create their WAN consumers when
 * they start, so the class must exist.
 */
public class WanConsumer implements uk.org.hecaton.wan.WanConsumer {

    @Override
    public void init(String wanReplicationName, WanConsumerConfig config) {
    }

    @Override
    public void shutdown() {
    }
}
