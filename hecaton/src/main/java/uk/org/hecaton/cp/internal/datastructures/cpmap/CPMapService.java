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
package uk.org.hecaton.cp.internal.datastructures.cpmap;

import uk.org.hecaton.config.cp.CPMapConfig;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import uk.org.hecaton.cp.internal.datastructures.spi.atomic.RaftAtomicValueService;
import uk.org.hecaton.internal.metrics.DynamicMetricsProvider;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsCollectionContext;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.BiTuple;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.CP_TAG_NAME;
import static uk.org.hecaton.spi.properties.ClusterProperty.METRICS_DATASTRUCTURES;

/**
 * Contains the {@link CPMap} state machines of the Raft groups on this member, implements
 * snapshotting and creates proxies.
 */
public class CPMapService extends RaftAtomicValueService<Map<Data, CPMapEntry>, CPMapContainer, CPMapSnapshot>
        implements DynamicMetricsProvider {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = CPMapServiceUtil.SERVICE_NAME;

    private static final long BYTES_PER_MB = 1024L * 1024L;

    public CPMapService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        super.init(nodeEngine, properties);
        if (nodeEngine.getProperties().getBoolean(METRICS_DATASTRUCTURES)) {
            ((NodeEngineImpl) nodeEngine).getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
    }

    /**
     * Returns the configuration of the given CPMap, or the default one.
     */
    public CPMapConfig getConfig(String name) {
        CPMapConfig config = nodeEngine.getConfig().getCPSubsystemConfig().findCPMapConfig(name);
        return config != null ? config : new CPMapConfig(name);
    }

    /**
     * Returns the CPMap if an operation has created it and it has not been destroyed, otherwise null.
     */
    public CPMapContainer getExistingContainer(CPGroupId groupId, String name) {
        return atomicValues.get(BiTuple.of(groupId, name));
    }

    @Override
    protected CPMapSnapshot newSnapshot(Map<String, Map<Data, CPMapEntry>> values, Set<String> destroyed) {
        return new CPMapSnapshot(values, destroyed);
    }

    @Override
    protected CPMapContainer newAtomicValue(CPGroupId groupId, String name, Map<Data, CPMapEntry> entries) {
        return new CPMapContainer(groupId, name, entries, getConfig(name).getMaxSizeMb() * BYTES_PER_MB);
    }

    @Override
    protected CPMap<?, ?> newRaftAtomicProxy(NodeEngineImpl nodeEngine, RaftGroupId groupId, String proxyName,
                                             String objectNameForProxy) {
        return new CPMapProxy<>(nodeEngine, groupId, proxyName, objectNameForProxy);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor root = descriptor.withPrefix("cp.map");
        for (CPMapContainer map : atomicValues.values()) {
            CPGroupId groupId = map.groupId();
            MetricDescriptor desc = root.copy()
                    .withDiscriminator("id", map.name() + "@" + groupId.getName())
                    .withTag(CP_TAG_NAME, map.name())
                    .withTag("group", groupId.getName());
            context.collect(desc.copy().withMetric("size"), map.size());
            context.collect(desc.copy().withMetric("sizeBytes"), map.sizeBytes());
        }
    }
}
