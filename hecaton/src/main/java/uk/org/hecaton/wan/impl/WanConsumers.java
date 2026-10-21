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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.wan.WanConsumer;

import java.util.HashMap;
import java.util.Map;

import static uk.org.hecaton.internal.nio.ClassLoaderUtil.getOrCreate;

/** The custom WAN consumers of a member, by WAN replication name. Accessed under the service's lock. */
class WanConsumers {

    private final Node node;
    private final Map<String, WanConsumer> consumers = new HashMap<>();

    WanConsumers(Node node) {
        this.node = node;
    }

    /** Creates and initializes the consumer of a WAN replication config, if it names one. */
    void start(WanReplicationConfig config) {
        WanConsumerConfig consumerConfig = config.getConsumerConfig();
        if (consumerConfig == null || consumers.containsKey(config.getName())
                || consumerConfig.getImplementation() == null && consumerConfig.getClassName() == null) {
            return;
        }
        WanConsumer consumer;
        try {
            consumer = getOrCreate(consumerConfig.getImplementation(), node.getConfigClassLoader(),
                    consumerConfig.getClassName());
        } catch (Exception e) {
            throw new InvalidConfigurationException("Cannot create the WAN consumer " + consumerConfig.getClassName(), e);
        }
        consumer = (WanConsumer) node.getSerializationService().getManagedContext().initialize(consumer);
        consumer.init(config.getName(), consumerConfig);
        consumers.put(config.getName(), consumer);
    }

    void shutdown() {
        consumers.values().forEach(WanConsumer::shutdown);
        consumers.clear();
    }
}
