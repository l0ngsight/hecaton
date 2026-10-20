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

package uk.org.hecaton.internal.dynamicconfig;

import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CardinalityEstimatorConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigXmlGenerator;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.config.DataConnectionConfig;
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
import uk.org.hecaton.config.TcpIpConfig;
import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.config.UserCodeNamespaceConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.internal.util.XmlUtil;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static uk.org.hecaton.internal.config.ConfigSections.CACHE;
import static uk.org.hecaton.internal.config.ConfigSections.CARDINALITY_ESTIMATOR;
import static uk.org.hecaton.internal.config.ConfigSections.DURABLE_EXECUTOR_SERVICE;
import static uk.org.hecaton.internal.config.ConfigSections.EXECUTOR_SERVICE;
import static uk.org.hecaton.internal.config.ConfigSections.DATA_CONNECTION;
import static uk.org.hecaton.internal.config.ConfigSections.FLAKE_ID_GENERATOR;
import static uk.org.hecaton.internal.config.ConfigSections.LIST;
import static uk.org.hecaton.internal.config.ConfigSections.MAP;
import static uk.org.hecaton.internal.config.ConfigSections.MULTIMAP;
import static uk.org.hecaton.internal.config.ConfigSections.PN_COUNTER;
import static uk.org.hecaton.internal.config.ConfigSections.QUEUE;
import static uk.org.hecaton.internal.config.ConfigSections.RELIABLE_TOPIC;
import static uk.org.hecaton.internal.config.ConfigSections.REPLICATED_MAP;
import static uk.org.hecaton.internal.config.ConfigSections.RINGBUFFER;
import static uk.org.hecaton.internal.config.ConfigSections.SCHEDULED_EXECUTOR_SERVICE;
import static uk.org.hecaton.internal.config.ConfigSections.SET;
import static uk.org.hecaton.internal.config.ConfigSections.TCP_IP;
import static uk.org.hecaton.internal.config.ConfigSections.TOPIC;
import static uk.org.hecaton.internal.config.ConfigSections.VECTOR;
import static uk.org.hecaton.internal.config.ConfigSections.WAN_REPLICATION;

public final class DynamicConfigGeneratorUtil {

    private DynamicConfigGeneratorUtil() {
        // not called
    }

    public static String licenseKeyConfigGenerator(
            String subConfig, boolean configIsXml, int indent
    ) {
        return configGenerator(subConfig, configIsXml, indent,
                null,
                Config::setLicenseKey,
                DynamicConfigXmlGenerator::licenseKeyXmlGenerator,
                DynamicConfigYamlGenerator::licenseKeyYamlGenerator
        );
    }

    public static String wanReplicationConfigGenerator(
            WanReplicationConfig subConfig, boolean configIsXml, int indent
    ) {
        return configGenerator(subConfig, configIsXml, indent,
                WAN_REPLICATION.getName(),
                Config::addWanReplicationConfig,
                DynamicConfigXmlGenerator::wanReplicationXmlGenerator,
                DynamicConfigYamlGenerator::wanReplicationYamlGenerator
        );
    }

    public static String mapConfigGenerator(MapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                MAP.getName(),
                Config::addMapConfig,
                DynamicConfigXmlGenerator::mapXmlGenerator,
                DynamicConfigYamlGenerator::mapYamlGenerator
        );
    }

    public static String cacheConfigGenerator(CacheSimpleConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                CACHE.getName(),
                Config::addCacheConfig,
                DynamicConfigXmlGenerator::cacheXmlGenerator,
                DynamicConfigYamlGenerator::cacheYamlGenerator
        );
    }

    public static String queueConfigGenerator(QueueConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                QUEUE.getName(),
                Config::addQueueConfig,
                DynamicConfigXmlGenerator::queueXmlGenerator,
                DynamicConfigYamlGenerator::queueYamlGenerator
        );
    }

    public static String listConfigGenerator(ListConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                LIST.getName(),
                Config::addListConfig,
                DynamicConfigXmlGenerator::listXmlGenerator,
                DynamicConfigYamlGenerator::listYamlGenerator
        );
    }

    public static String setConfigGenerator(SetConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                SET.getName(),
                Config::addSetConfig,
                DynamicConfigXmlGenerator::setXmlGenerator,
                DynamicConfigYamlGenerator::setYamlGenerator
        );
    }

    public static String multiMapConfigGenerator(MultiMapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                MULTIMAP.getName(),
                Config::addMultiMapConfig,
                DynamicConfigXmlGenerator::multiMapXmlGenerator,
                DynamicConfigYamlGenerator::multiMapYamlGenerator
        );
    }

    public static String replicatedMapConfigGenerator(ReplicatedMapConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                REPLICATED_MAP.getName(),
                Config::addReplicatedMapConfig,
                DynamicConfigXmlGenerator::replicatedMapXmlGenerator,
                DynamicConfigYamlGenerator::replicatedMapYamlGenerator
        );
    }

    public static String ringbufferConfigGenerator(RingbufferConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                RINGBUFFER.getName(),
                Config::addRingBufferConfig,
                DynamicConfigXmlGenerator::ringbufferXmlGenerator,
                DynamicConfigYamlGenerator::ringbufferYamlGenerator
        );
    }

    public static String topicConfigGenerator(TopicConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                TOPIC.getName(),
                Config::addTopicConfig,
                DynamicConfigXmlGenerator::topicXmlGenerator,
                DynamicConfigYamlGenerator::topicYamlGenerator
        );
    }

    public static String reliableTopicConfigGenerator(ReliableTopicConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                RELIABLE_TOPIC.getName(),
                Config::addReliableTopicConfig,
                DynamicConfigXmlGenerator::reliableTopicXmlGenerator,
                DynamicConfigYamlGenerator::reliableTopicYamlGenerator
        );
    }

    public static String executorConfigGenerator(ExecutorConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                EXECUTOR_SERVICE.getName(),
                Config::addExecutorConfig,
                DynamicConfigXmlGenerator::executorXmlGenerator,
                DynamicConfigYamlGenerator::executorYamlGenerator
        );
    }

    public static String durableExecutorConfigGenerator(
            DurableExecutorConfig subConfig, boolean configIsXml, int indent
    ) {
        return configGenerator(subConfig, configIsXml, indent,
                DURABLE_EXECUTOR_SERVICE.getName(),
                Config::addDurableExecutorConfig,
                DynamicConfigXmlGenerator::durableExecutorXmlGenerator,
                DynamicConfigYamlGenerator::durableExecutorYamlGenerator
        );
    }

    public static String scheduledExecutorConfigGenerator(
            ScheduledExecutorConfig subConfig, boolean configIsXml, int indent
    ) {
        return configGenerator(subConfig, configIsXml, indent,
                SCHEDULED_EXECUTOR_SERVICE.getName(),
                Config::addScheduledExecutorConfig,
                DynamicConfigXmlGenerator::scheduledExecutorXmlGenerator,
                DynamicConfigYamlGenerator::scheduledExecutorYamlGenerator
        );
    }

    public static String cardinalityEstimatorConfigGenerator(
            CardinalityEstimatorConfig subConfig, boolean configIsXml, int indent
    ) {
        return configGenerator(subConfig, configIsXml, indent,
                CARDINALITY_ESTIMATOR.getName(),
                Config::addCardinalityEstimatorConfig,
                DynamicConfigXmlGenerator::cardinalityEstimatorXmlGenerator,
                DynamicConfigYamlGenerator::cardinalityEstimatorYamlGenerator
        );
    }

    public static String flakeIdGeneratorConfigGenerator(
            FlakeIdGeneratorConfig subConfig, boolean configIsXml, int indent
    ) {
        return configGenerator(subConfig, configIsXml, indent,
                FLAKE_ID_GENERATOR.getName(),
                Config::addFlakeIdGeneratorConfig,
                DynamicConfigXmlGenerator::flakeIdGeneratorXmlGenerator,
                DynamicConfigYamlGenerator::flakeIdGeneratorYamlGenerator
        );
    }

    public static String pnCounterConfigGenerator(PNCounterConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                PN_COUNTER.getName(),
                Config::addPNCounterConfig,
                DynamicConfigXmlGenerator::pnCounterXmlGenerator,
                DynamicConfigYamlGenerator::pnCounterYamlGenerator
        );
    }

    public static String dataConnectionConfigGenerator(DataConnectionConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent,
                DATA_CONNECTION.getName(),
                Config::addDataConnectionConfig,
                DynamicConfigXmlGenerator::dataConnectionXmlGenerator,
                DynamicConfigYamlGenerator::dataConnectionYamlGenerator
        );
    }

    public static String tcpIpConfigGenerator(
            TcpIpConfig subConfig, boolean configIsXml, int indent, boolean advancedNetwork
    ) {
        return configGenerator(subConfig, configIsXml, indent,
                TCP_IP.getName(),
                (config, tcpIpConfig) -> {
                    if (advancedNetwork) {
                        config.getAdvancedNetworkConfig().getJoin().setTcpIpConfig(tcpIpConfig);
                    } else {
                        config.getNetworkConfig().getJoin().setTcpIpConfig(tcpIpConfig);
                    }
                },
                DynamicConfigXmlGenerator::tcpIpConfigXmlGenerator,
                DynamicConfigYamlGenerator::tcpIpConfigYamlGenerator
        );
    }

    public static String userCodeNamespaceConfigGenerator(UserCodeNamespaceConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(subConfig, configIsXml, indent, null,
                (config, userCodeNamespaceConfig) -> {
                    // assumes namespaces are already enabled declaratively
                    config.getNamespacesConfig().setEnabled(true);
                    config.getNamespacesConfig().addNamespaceConfig(userCodeNamespaceConfig);
                },
                ConfigXmlGenerator::namespaceConfigurations, DynamicConfigYamlGenerator::namespaceConfigGenerator);
    }

    public static String vectorCollectionConfigGenerator(VectorCollectionConfig subConfig, boolean configIsXml, int indent) {
        return configGenerator(
                subConfig,
                configIsXml,
                indent,
                VECTOR.getName(),
                Config::addVectorCollectionConfig,
                DynamicConfigXmlGenerator::vectorCollectionXmlGenerator,
                DynamicConfigYamlGenerator::vectorCollectionYamlGenerator
        );
    }

    private static <T> String configGenerator(
            T subConfig,
            boolean configIsXml,
            int indent,
            String subConfigTypeName,
            BiConsumer<Config, T> subConfigAdder,
            BiConsumer<ConfigXmlGenerator.XmlGenerator, Config> xmlConfigGenerator,
            BiConsumer<Map<String, Object>, Config> yamlConfigGenerator
    ) {

        String subConfigAsString;

        Config config = new Config();
        subConfigAdder.accept(config, subConfig);

        if (configIsXml) {
            StringBuilder xml = new StringBuilder();
            ConfigXmlGenerator.XmlGenerator gen = new ConfigXmlGenerator.XmlGenerator(xml);

            xmlConfigGenerator.accept(gen, config);

            subConfigAsString = xml.toString();
            subConfigAsString = XmlUtil.format(subConfigAsString, indent);
        } else {
            Map<String, Object> yaml = new LinkedHashMap<>();

            yamlConfigGenerator.accept(yaml, config);

            DumpSettings dumpSettings = DumpSettings.builder()
                    .setDefaultFlowStyle(FlowStyle.BLOCK)
                    .setIndicatorIndent(indent - 2)
                    .setIndent(indent)
                    .build();
            Dump dump = new Dump(dumpSettings);
            subConfigAsString = dump.dumpToString(subConfigTypeName == null ? yaml : yaml.get(subConfigTypeName));
        }

        return subConfigAsString;
    }
}
