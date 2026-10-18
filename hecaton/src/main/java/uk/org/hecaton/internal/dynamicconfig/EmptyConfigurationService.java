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
import uk.org.hecaton.internal.diagnostics.DiagnosticsConfig;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.config.FlakeIdGeneratorConfig;
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.UserCodeNamespaceConfig;
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
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyMap;

/**
 * This is used when Hecaton is starting and {@link ClusterWideConfigurationService} is not available yet.
 */
@SuppressWarnings("checkstyle:methodcount")
class EmptyConfigurationService implements ConfigurationService {

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        return null;
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return null;
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        return null;
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return null;
    }

    @Override
    public PNCounterConfig findPNCounterConfig(String name) {
        return null;
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return null;
    }

    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return null;
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return null;
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        return null;
    }

    @Override
    public ListConfig findListConfig(String name) {
        return null;
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        return null;
    }

    @Override
    public SetConfig findSetConfig(String name) {
        return null;
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return null;
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return null;
    }

    @Override
    public CacheSimpleConfig findCacheSimpleConfig(String name) {
        return null;
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheSimpleConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ListConfig> getListConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, SetConfig> getSetConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, TopicConfig> getTopicConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, PNCounterConfig> getPNCounterConfigs() {
        return emptyMap();
    }

    @Override
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String baseName) {
        return null;
    }

    @Override
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        return emptyMap();
    }

    @Override
    public DataConnectionConfig findDataConnectionConfig(String baseName) {
        return null;
    }

    @Override
    public Map<String, DataConnectionConfig> getDataConnectionConfigs() {
        return emptyMap();
    }

    @Override
    public WanReplicationConfig findWanReplicationConfig(String name) {
        return null;
    }

    @Override
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return emptyMap();
    }

    @Override
    public Map<String, UserCodeNamespaceConfig> getNamespaceConfigs() {
        return emptyMap();
    }

    @Override
    public VectorCollectionConfig findVectorCollectionConfig(String name) {
        return null;
    }

    @Override
    public Map<String, VectorCollectionConfig> getVectorCollectionConfigs() {
        return emptyMap();
    }

    @Override
    public DiagnosticsConfig getDiagnosticsConfig() {
        return null;
    }

    @Override
    public void broadcastConfig(IdentifiedDataSerializable config) {
        throw new IllegalStateException("Cannot modify configuration while Hecaton is starting.");
    }

    @Override
    public void persist(Object subConfig) {
        // Code shouldn't come here. broadcastConfig() will throw an exception
        // before here.
        throw new IllegalStateException("Cannot add a new config while Hecaton is starting.");
    }

    @Override
    public ConfigUpdateResult update(Config newConfig) {
        throw new IllegalStateException("Cannot reload config while Hecaton is starting.");
    }

    @Override
    public UUID updateAsync(String configPatch) {
        throw new IllegalStateException("Cannot reload config while Hecaton is starting.");
    }

    @Override
    public void updateLicense(String licenseKey) {
        throw new IllegalStateException("Cannot update license while Hecaton is starting.");
    }

    @Override
    public CompletableFuture<Void> updateLicenseAsync(String licenseKey) {
        throw new IllegalStateException("Cannot update license while Hecaton is starting.");
    }

    @Override
    public void updateTcpIpConfigMemberList(List<String> memberList) {
        throw new IllegalStateException("Cannot update the member list of TCP-IP join config while Hecaton is starting.");
    }

    @Override
    public CompletableFuture<Void> updateTcpIpConfigMemberListAsync(List<String> memberList) {
        throw new IllegalStateException("Cannot update the member list of TCP-IP join config while Hecaton is starting.");
    }
}
