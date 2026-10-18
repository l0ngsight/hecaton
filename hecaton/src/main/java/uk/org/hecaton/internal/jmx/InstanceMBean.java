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

package uk.org.hecaton.internal.jmx;

import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.monitor.LocalWanPublisherStats;
import uk.org.hecaton.internal.monitor.LocalWanStats;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.wan.impl.WanReplicationService;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static uk.org.hecaton.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static uk.org.hecaton.internal.jmx.ManagementService.quote;
import static uk.org.hecaton.internal.util.MapUtil.createHashMap;

/**
 * Management bean for {@link uk.org.hecaton.core.HecatonInstance}
 */
@ManagedDescription("HecatonInstance")
@SuppressWarnings("checkstyle:methodcount")
public class InstanceMBean extends HecatonMBean<HecatonInstanceImpl> {

    private static final int INITIAL_CAPACITY = 3;

    final Config config;
    final Cluster cluster;
    private NodeMBean nodeMBean;
    private ManagedExecutorServiceMBean systemExecutorMBean;
    private ManagedExecutorServiceMBean asyncExecutorMBean;
    private ManagedExecutorServiceMBean scheduledExecutorMBean;
    private ManagedExecutorServiceMBean clientExecutorMBean;
    private ManagedExecutorServiceMBean clientQueryExecutorMBean;
    private ManagedExecutorServiceMBean clientBlockingExecutorMBean;
    private ManagedExecutorServiceMBean queryExecutorMBean;
    private ManagedExecutorServiceMBean ioExecutorMBean;
    private ManagedExecutorServiceMBean offloadableExecutorMBean;
    private PartitionServiceMBean partitionServiceMBean;
    private LoggingServiceMBean loggingServiceMBean;

    protected InstanceMBean(HecatonInstanceImpl hecatonInstance, ManagementService managementService) {
        super(hecatonInstance, managementService);
        createProperties(hecatonInstance);
        this.config = hecatonInstance.getConfig();
        this.cluster = hecatonInstance.getCluster();
        Node node = hecatonInstance.node;
        ExecutionService executionService = node.nodeEngine.getExecutionService();
        createMBeans(hecatonInstance, managementService, node, executionService);
        registerMBeans();
        registerWanPublisherMBeans(node.nodeEngine.getWanReplicationService());
    }

    /**
     * Registers managed beans for all WAN publishers, if any.
     *
     * @param wanReplicationService the WAN replication service
     */
    private void registerWanPublisherMBeans(WanReplicationService wanReplicationService) {
        final Map<String, LocalWanStats> wanStats = wanReplicationService.getStats();
        if (wanStats == null) {
            return;
        }

        for (Entry<String, LocalWanStats> replicationStatsEntry : wanStats.entrySet()) {
            final String wanReplicationName = replicationStatsEntry.getKey();
            final LocalWanStats localWanStats = replicationStatsEntry.getValue();
            final Map<String, LocalWanPublisherStats> publisherStats = localWanStats.getLocalWanPublisherStats();

            for (String wanPublisherId : publisherStats.keySet()) {
                register(new WanPublisherMBean(wanReplicationService, wanReplicationName, wanPublisherId, service));
            }
        }
    }

    private void createMBeans(HecatonInstanceImpl hecatonInstance, ManagementService managementService, Node node,
                              ExecutionService executionService) {
        this.nodeMBean = new NodeMBean(hecatonInstance, node, managementService);
        this.partitionServiceMBean = new PartitionServiceMBean(hecatonInstance, node.partitionService, service);
        this.systemExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.SYSTEM_EXECUTOR), service);
        this.asyncExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR), service);
        this.scheduledExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.SCHEDULED_EXECUTOR), service);
        this.clientExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.CLIENT_EXECUTOR), service);
        this.clientQueryExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.CLIENT_QUERY_EXECUTOR), service);
        this.clientBlockingExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.CLIENT_BLOCKING_EXECUTOR), service);
        this.queryExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.QUERY_EXECUTOR), service);
        this.ioExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.IO_EXECUTOR), service);
        this.offloadableExecutorMBean = new ManagedExecutorServiceMBean(
                hecatonInstance, executionService.getExecutor(ExecutionService.OFFLOADABLE_EXECUTOR), service);
        this.loggingServiceMBean = new LoggingServiceMBean(hecatonInstance, service);
    }

    private void registerMBeans() {
        register(nodeMBean);
        register(partitionServiceMBean);
        register(systemExecutorMBean);
        register(asyncExecutorMBean);
        register(scheduledExecutorMBean);
        register(clientExecutorMBean);
        register(clientQueryExecutorMBean);
        register(clientBlockingExecutorMBean);
        register(queryExecutorMBean);
        register(ioExecutorMBean);
        register(offloadableExecutorMBean);
        register(loggingServiceMBean);
    }

    private void createProperties(HecatonInstanceImpl hecatonInstance) {
        final Map<String, String> properties = createHashMap(INITIAL_CAPACITY);
        properties.put("type", quote("HecatonInstance"));
        properties.put("instance", quote(hecatonInstance.getName()));
        properties.put("name", quote(hecatonInstance.getName()));
        setObjectName(properties);
    }

    public PartitionServiceMBean getPartitionServiceMBean() {
        return partitionServiceMBean;
    }

    public ManagedExecutorServiceMBean getSystemExecutorMBean() {
        return systemExecutorMBean;
    }

    public ManagedExecutorServiceMBean getAsyncExecutorMBean() {
        return asyncExecutorMBean;
    }

    public ManagedExecutorServiceMBean getScheduledExecutorMBean() {
        return scheduledExecutorMBean;
    }

    public ManagedExecutorServiceMBean getClientExecutorMBean() {
        return clientExecutorMBean;
    }

    public ManagedExecutorServiceMBean getClientQueryExecutorMBean() {
        return clientQueryExecutorMBean;
    }

    public ManagedExecutorServiceMBean getClientBlockingExecutorMBean() {
        return clientBlockingExecutorMBean;
    }

    public ManagedExecutorServiceMBean getQueryExecutorMBean() {
        return queryExecutorMBean;
    }

    public ManagedExecutorServiceMBean getIoExecutorMBean() {
        return ioExecutorMBean;
    }

    public ManagedExecutorServiceMBean getOffloadableExecutorMBean() {
        return offloadableExecutorMBean;
    }

    public NodeMBean getNodeMBean() {
        return nodeMBean;
    }

    public HecatonInstance getHecatonInstance() {
        return managedObject;
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the Instance")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("version")
    @ManagedDescription("The Hecaton version")
    public String getVersion() {
        return managedObject.node.getBuildInfo().getVersion();
    }

    @ManagedAnnotation("build")
    @ManagedDescription("The Hecaton build")
    public String getBuild() {
        return managedObject.node.getBuildInfo().getBuild();
    }

    @ManagedAnnotation("config")
    @ManagedDescription("String representation of config")
    public String getConfig() {
        return config.toString();
    }

    @ManagedAnnotation("configSource")
    @ManagedDescription("The source of config")
    public String getConfigSource() {
        File configurationFile = config.getConfigurationFile();
        if (configurationFile != null) {
            return configurationFile.getAbsolutePath();
        }
        URL configurationUrl = config.getConfigurationUrl();
        if (configurationUrl != null) {
            return configurationUrl.toString();
        }
        return null;
    }

    @ManagedAnnotation("clusterName")
    @ManagedDescription("Cluster Name")
    public String getClusterName() {
        return config.getClusterName();
    }

    @ManagedAnnotation("port")
    @ManagedDescription("Network Port")
    public int getPort() {
        return getActiveMemberNetworkConfig(config).getPort();
    }

    @ManagedAnnotation("clusterTime")
    @ManagedDescription("Cluster-wide Time")
    public long getClusterTime() {
        return cluster.getClusterTime();
    }

    @ManagedAnnotation("memberCount")
    @ManagedDescription("size of the cluster")
    public int getMemberCount() {
        return cluster.getMembers().size();
    }

    @ManagedAnnotation("Members")
    @ManagedDescription("List of Members")
    public List<String> getMembers() {
        Set<Member> members = cluster.getMembers();
        List<String> list = new ArrayList<>(members.size());
        for (Member member : members) {
            list.add(member.getSocketAddress().toString());
        }
        return list;
    }

    @ManagedAnnotation("running")
    @ManagedDescription("Running state")
    public boolean isRunning() {
        return managedObject.getLifecycleService().isRunning();
    }

    @ManagedAnnotation(value = "shutdown", operation = true)
    @ManagedDescription("Shutdown the Node")
    public void shutdown() {
        managedObject.getLifecycleService().shutdown();
    }

}
