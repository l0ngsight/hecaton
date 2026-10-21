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

package uk.org.hecaton.internal.dynamicconfig;

import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CardinalityEstimatorConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.config.FlakeIdGeneratorConfig;
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.PNCounterConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.config.ReliableTopicConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.ScheduledExecutorConfig;
import uk.org.hecaton.config.SetConfig;
import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.internal.config.ConfigSections;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A kind of named data structure configuration that can be added at runtime: its section in the
 * declarative configuration, how to read and add it on a {@link Config}, and how to write it as
 * XML or YAML (F16: dynamic configuration persistence and configuration reload).
 *
 * @param section   the section of the declarative configuration
 * @param type      the configuration class
 * @param name      the name of a configuration
 * @param configs   the configurations of this kind in a {@code Config}, by name
 * @param adder     adds a configuration of this kind to a {@code Config}
 * @param generator writes a configuration as XML ({@code true}) or YAML, with the given indentation
 * @param <T>       the configuration class
 */
public record DynamicConfigSection<T>(ConfigSections section, Class<T> type, Function<T, String> name,
                                      Function<Config, Map<String, T>> configs,
                                      BiConsumer<Config, T> adder, Generator<T> generator) {

    /** The kinds of configuration that are persisted and reloaded. */
    public static final List<DynamicConfigSection<?>> ALL = List.of(
            new DynamicConfigSection<>(ConfigSections.MAP, MapConfig.class, MapConfig::getName,
                    Config::getMapConfigs, Config::addMapConfig,
                    DynamicConfigGeneratorUtil::mapConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.CACHE, CacheSimpleConfig.class, CacheSimpleConfig::getName,
                    Config::getCacheConfigs, Config::addCacheConfig,
                    DynamicConfigGeneratorUtil::cacheConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.QUEUE, QueueConfig.class, QueueConfig::getName,
                    Config::getQueueConfigs, Config::addQueueConfig,
                    DynamicConfigGeneratorUtil::queueConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.LIST, ListConfig.class, ListConfig::getName,
                    Config::getListConfigs, Config::addListConfig,
                    DynamicConfigGeneratorUtil::listConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.SET, SetConfig.class, SetConfig::getName,
                    Config::getSetConfigs, Config::addSetConfig,
                    DynamicConfigGeneratorUtil::setConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.MULTIMAP, MultiMapConfig.class, MultiMapConfig::getName,
                    Config::getMultiMapConfigs, Config::addMultiMapConfig,
                    DynamicConfigGeneratorUtil::multiMapConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.REPLICATED_MAP, ReplicatedMapConfig.class, ReplicatedMapConfig::getName,
                    Config::getReplicatedMapConfigs, Config::addReplicatedMapConfig,
                    DynamicConfigGeneratorUtil::replicatedMapConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.RINGBUFFER, RingbufferConfig.class, RingbufferConfig::getName,
                    Config::getRingbufferConfigs, Config::addRingBufferConfig,
                    DynamicConfigGeneratorUtil::ringbufferConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.TOPIC, TopicConfig.class, TopicConfig::getName,
                    Config::getTopicConfigs, Config::addTopicConfig,
                    DynamicConfigGeneratorUtil::topicConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.RELIABLE_TOPIC, ReliableTopicConfig.class, ReliableTopicConfig::getName,
                    Config::getReliableTopicConfigs, Config::addReliableTopicConfig,
                    DynamicConfigGeneratorUtil::reliableTopicConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.EXECUTOR_SERVICE, ExecutorConfig.class, ExecutorConfig::getName,
                    Config::getExecutorConfigs, Config::addExecutorConfig,
                    DynamicConfigGeneratorUtil::executorConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.DURABLE_EXECUTOR_SERVICE, DurableExecutorConfig.class,
                    DurableExecutorConfig::getName,
                    Config::getDurableExecutorConfigs, Config::addDurableExecutorConfig,
                    DynamicConfigGeneratorUtil::durableExecutorConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.SCHEDULED_EXECUTOR_SERVICE, ScheduledExecutorConfig.class,
                    ScheduledExecutorConfig::getName,
                    Config::getScheduledExecutorConfigs, Config::addScheduledExecutorConfig,
                    DynamicConfigGeneratorUtil::scheduledExecutorConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.CARDINALITY_ESTIMATOR, CardinalityEstimatorConfig.class,
                    CardinalityEstimatorConfig::getName,
                    Config::getCardinalityEstimatorConfigs, Config::addCardinalityEstimatorConfig,
                    DynamicConfigGeneratorUtil::cardinalityEstimatorConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.FLAKE_ID_GENERATOR, FlakeIdGeneratorConfig.class,
                    FlakeIdGeneratorConfig::getName,
                    Config::getFlakeIdGeneratorConfigs, Config::addFlakeIdGeneratorConfig,
                    DynamicConfigGeneratorUtil::flakeIdGeneratorConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.PN_COUNTER, PNCounterConfig.class, PNCounterConfig::getName,
                    Config::getPNCounterConfigs, Config::addPNCounterConfig,
                    DynamicConfigGeneratorUtil::pnCounterConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.DATA_CONNECTION, DataConnectionConfig.class, DataConnectionConfig::getName,
                    Config::getDataConnectionConfigs, Config::addDataConnectionConfig,
                    DynamicConfigGeneratorUtil::dataConnectionConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.WAN_REPLICATION, WanReplicationConfig.class, WanReplicationConfig::getName,
                    Config::getWanReplicationConfigs, Config::addWanReplicationConfig,
                    DynamicConfigGeneratorUtil::wanReplicationConfigGenerator),
            new DynamicConfigSection<>(ConfigSections.VECTOR, VectorCollectionConfig.class, VectorCollectionConfig::getName,
                    Config::getVectorCollectionConfigs, Config::addVectorCollectionConfig,
                    DynamicConfigGeneratorUtil::vectorCollectionConfigGenerator)
    );

    /** Writes a configuration as XML or YAML. */
    @FunctionalInterface
    public interface Generator<T> {
        String generate(T config, boolean xml, int indent);
    }

    /** The kind of a configuration object, or {@code null} if it is not one of {@link #ALL}. */
    @SuppressWarnings("unchecked")
    public static <T> DynamicConfigSection<T> of(Object config) {
        for (DynamicConfigSection<?> section : ALL) {
            if (section.type().isInstance(config)) {
                return (DynamicConfigSection<T>) section;
            }
        }
        return null;
    }

    /** The name of the configuration, which must be of this kind. */
    public String nameOf(Object config) {
        return name.apply(type.cast(config));
    }

    /** Writes the configuration, which must be of this kind. */
    public String generate(Object config, boolean xml, int indent) {
        return generator.generate(type.cast(config), xml, indent);
    }
}
