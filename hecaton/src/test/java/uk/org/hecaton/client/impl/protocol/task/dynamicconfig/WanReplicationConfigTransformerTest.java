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
package uk.org.hecaton.client.impl.protocol.task.dynamicconfig;

import uk.org.hecaton.client.impl.protocol.codec.holder.RemovedDiscoveryConfigHolder;
import uk.org.hecaton.client.impl.protocol.codec.holder.WanBatchPublisherConfigHolder;
import uk.org.hecaton.client.impl.protocol.codec.holder.WanConsumerConfigHolder;
import uk.org.hecaton.client.impl.protocol.codec.holder.WanCustomPublisherConfigHolder;
import uk.org.hecaton.config.ConsistencyCheckStrategy;
import uk.org.hecaton.config.DiscoveryConfig;
import uk.org.hecaton.config.DiscoveryStrategyConfig;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.KubernetesConfig;
import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.config.WanCustomPublisherConfig;
import uk.org.hecaton.config.WanQueueFullBehavior;
import uk.org.hecaton.config.WanSyncConfig;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.WanConsumer;
import uk.org.hecaton.wan.WanPublisher;
import uk.org.hecaton.wan.WanPublisherState;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanReplicationConfigTransformerTest {
    private static final String TEST_CLASS_NAME = "uk.org.hecaton.Test";
    private SerializationService serializationService;
    private WanReplicationConfigTransformer transformer;

    @Before
    public void before() {
        serializationService = Mockito.mock(SerializationService.class);
        transformer = new WanReplicationConfigTransformer(serializationService);
    }

    private Map<String, Comparable> mockProperties() {
        String stringValue = "test";
        Data stringData = Mockito.mock(Data.class);
        Mockito.when(serializationService.toData(stringValue)).thenReturn(stringData);
        Mockito.when(serializationService.toObject(stringData)).thenReturn(stringValue);

        int intValue = 1;
        Data intData = Mockito.mock(Data.class);
        Mockito.when(serializationService.toData(intValue)).thenReturn(intData);
        Mockito.when(serializationService.toObject(intData)).thenReturn(intValue);

        Map<String, Comparable> properties = new HashMap<>();
        properties.put("a", stringValue);
        properties.put("b", intValue);
        return properties;
    }

    @Test
    public void testGetWanPublisherState() {
        assertEquals(WanPublisherState.REPLICATING, transformer.getWanPublisherState((byte) 0));
        assertEquals(WanPublisherState.PAUSED, transformer.getWanPublisherState((byte) 1));
        assertEquals(WanPublisherState.STOPPED, transformer.getWanPublisherState((byte) 2));
        assertEquals(WanBatchPublisherConfig.DEFAULT_INITIAL_PUBLISHER_STATE, transformer.getWanPublisherState((byte) 3));
    }

    @Test
    public void testGetWanQueueFullBehaviour() {
        assertEquals(WanQueueFullBehavior.DISCARD_AFTER_MUTATION, transformer.getWanQueueFullBehaviour(0));
        assertEquals(WanQueueFullBehavior.THROW_EXCEPTION, transformer.getWanQueueFullBehaviour(1));
        assertEquals(WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE, transformer.getWanQueueFullBehaviour(2));
        assertEquals(WanBatchPublisherConfig.DEFAULT_QUEUE_FULL_BEHAVIOUR, transformer.getWanQueueFullBehaviour(3));
    }

    @Test
    public void testGetWanAcknowledgeType() {
        assertEquals(WanAcknowledgeType.ACK_ON_RECEIPT, transformer.getWanAcknowledgeType(0));
        assertEquals(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE, transformer.getWanAcknowledgeType(1));
        assertEquals(WanBatchPublisherConfig.DEFAULT_ACKNOWLEDGE_TYPE, transformer.getWanAcknowledgeType(2));
    }

    @Test
    public void testWanConsumerConfigDefault() {
        WanConsumerConfig expected = new WanConsumerConfig();
        WanConsumerConfigHolder holder = transformer.toHolder(expected);
        WanConsumerConfig actual = transformer.toConfig(holder);
        assertEquals(expected, actual);
    }


    @Test
    public void testWanConsumerConfigCustomClassName() {
        testWanConsumerConfigCustom(false);
    }

    @Test
    public void testWanConsumerConfigCustomImplementation() {
        testWanConsumerConfigCustom(true);
    }

    private void testWanConsumerConfigCustom(boolean useImplementation) {
        WanConsumerConfig expected = new WanConsumerConfig();

        if (useImplementation) {
            Data wanConsumerData = Mockito.mock(Data.class);
            WanConsumer wanConsumer = Mockito.mock(WanConsumer.class);
            Mockito.when(serializationService.toData(wanConsumer)).thenReturn(wanConsumerData);
            Mockito.when(serializationService.toObject(wanConsumerData)).thenReturn(wanConsumer);
            expected.setImplementation(wanConsumer);
        } else {
            expected.setClassName(TEST_CLASS_NAME);
        }

        expected.setProperties(mockProperties());
        expected.setPersistWanReplicatedData(true);

        WanConsumerConfigHolder holder = transformer.toHolder(expected);
        WanConsumerConfig actual = transformer.toConfig(holder);
        assertEquals(expected, actual);
    }

    @Test
    public void testWanCustomPublisherDefault() {
        WanCustomPublisherConfig expected = new WanCustomPublisherConfig();
        WanCustomPublisherConfigHolder holder = transformer.toHolder(expected);
        WanCustomPublisherConfig actual = transformer.toConfig(holder);
        assertEquals(expected, actual); // equality is on the fields of interest
    }

    @Test
    public void testWanCustomPublisherConfigCustomClassName() {
        testWanCustomPublisherConfigCustom(false);
    }

    @Test
    public void testWanCustomPublisherConfigCustomImplementation() {
        testWanCustomPublisherConfigCustom(true);
    }

    private void testWanCustomPublisherConfigCustom(boolean useImplementation) {
        WanCustomPublisherConfig expected = new WanCustomPublisherConfig();
        expected.setPublisherId("my-publisher-id");

        if (useImplementation) {
            Data wanPublisherData = Mockito.mock(Data.class);
            WanPublisher wanPublisher = Mockito.mock(WanPublisher.class);
            Mockito.when(serializationService.toData(wanPublisher)).thenReturn(wanPublisherData);
            Mockito.when(serializationService.toObject(wanPublisherData)).thenReturn(wanPublisher);
            expected.setImplementation(wanPublisher);
        } else {
            expected.setClassName(TEST_CLASS_NAME);
        }
        expected.setProperties(mockProperties());
        WanCustomPublisherConfigHolder holder = transformer.toHolder(expected);
        WanCustomPublisherConfig actual = transformer.toConfig(holder);
        assertEquals(expected, actual);
    }

    @Test
    public void testWanBatchPublisherConfigDefault() {
        WanBatchPublisherConfig expected = new WanBatchPublisherConfig();
        WanBatchPublisherConfigHolder holder = transformer.toHolder(expected);
        WanBatchPublisherConfig actual = transformer.toConfig(holder);
        assertWanBatchPublisherEqual(expected, actual);
    }

    @Test
    public void testWanBatchPublisherConfigCustomClassName() {
        testWanBatchPublisherConfigCustom(false);
    }

    @Test
    public void testWanBatchPublisherConfigWithImplementation() {
        testWanBatchPublisherConfigCustom(true);
    }

    private static void assertWanBatchPublisherEqual(WanBatchPublisherConfig expected, WanBatchPublisherConfig actual) {
        assertEquals(expected, actual); // pub id, impl, classname + properties

        assertEquals(expected.getClusterName(), actual.getClusterName());
        assertEquals(expected.getInitialPublisherState(), actual.getInitialPublisherState());
        assertEquals(expected.getQueueCapacity(), actual.getQueueCapacity());
        assertEquals(expected.getBatchSize(), actual.getBatchSize());
        assertEquals(expected.getBatchMaxDelayMillis(), actual.getBatchMaxDelayMillis());
        assertEquals(expected.getResponseTimeoutMillis(), actual.getResponseTimeoutMillis());
        assertEquals(expected.getQueueFullBehavior(), actual.getQueueFullBehavior());
        assertEquals(expected.getAcknowledgeType(), actual.getAcknowledgeType());
        assertEquals(expected.getDiscoveryPeriodSeconds(), actual.getDiscoveryPeriodSeconds());
        assertEquals(expected.getMaxTargetEndpoints(), actual.getMaxTargetEndpoints());
        assertEquals(expected.getMaxConcurrentInvocations(), actual.getMaxConcurrentInvocations());
        assertEquals(expected.isUseEndpointPrivateAddress(), actual.isUseEndpointPrivateAddress());
        assertEquals(expected.getIdleMinParkNs(), actual.getIdleMinParkNs());
        assertEquals(expected.getIdleMaxParkNs(), actual.getIdleMaxParkNs());
        assertEquals(expected.getTargetEndpoints(), actual.getTargetEndpoints());
        assertEquals(expected.getEndpoint(), actual.getEndpoint());

        assertEquals(expected.getKubernetesConfig(), actual.getKubernetesConfig());

        assertEquals(expected.getDiscoveryConfig().getNodeFilterClass(), actual.getDiscoveryConfig().getNodeFilterClass());
        assertEquals(expected.getDiscoveryConfig().getNodeFilter(), actual.getDiscoveryConfig().getNodeFilter());
        assertEquals(expected.getDiscoveryConfig().getDiscoveryServiceProvider(), actual.getDiscoveryConfig().getDiscoveryServiceProvider());

        assertEquals(expected.getDiscoveryConfig(), actual.getDiscoveryConfig());
        assertEquals(expected.getSyncConfig(), actual.getSyncConfig());
    }

    private void testWanBatchPublisherConfigCustom(boolean useImplementation) {
        WanBatchPublisherConfig expected = new WanBatchPublisherConfig();
        expected.setClusterName(UUID.randomUUID().toString());
        expected.setPublisherId(UUID.randomUUID().toString());
        expected.setProperties(mockProperties());

        if (useImplementation) {
            Data wanPublisherData = Mockito.mock(Data.class);
            WanPublisher wanPublisher = Mockito.mock(WanPublisher.class);
            Mockito.when(serializationService.toData(wanPublisher)).thenReturn(wanPublisherData);
            Mockito.when(serializationService.toObject(wanPublisherData)).thenReturn(wanPublisher);
            expected.setImplementation(wanPublisher);
        } else {
            expected.setClassName(TEST_CLASS_NAME);
        }


        expected.setInitialPublisherState(WanPublisherState.PAUSED);
        expected.setEndpoint("my-endpoint");
        expected.setIdleMinParkNs(1_00L);
        expected.setIdleMaxParkNs(1_000L);
        expected.setUseEndpointPrivateAddress(true);
        expected.setMaxTargetEndpoints(10);
        expected.setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE);
        expected.setQueueCapacity(10_000);
        expected.setBatchSize(50);
        expected.setBatchMaxDelayMillis(100);
        expected.setResponseTimeoutMillis(5_000);
        expected.setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE);
        expected.setDiscoveryPeriodSeconds(1);
        expected.setMaxConcurrentInvocations(5);
        expected.setTargetEndpoints("a,b,c");
        expected.setSnapshotEnabled(true);

        Map<String, String> cloudProperties = new HashMap<>();
        cloudProperties.put("cloud-key", "cloud-value");
        KubernetesConfig k8sConfig = new KubernetesConfig("k8s-test", true, true, cloudProperties);
        expected.setKubernetesConfig(k8sConfig);
        WanSyncConfig wanSyncConfig = new WanSyncConfig();
        wanSyncConfig.setConsistencyCheckStrategy(ConsistencyCheckStrategy.MERKLE_TREES);
        expected.setSyncConfig(wanSyncConfig);

        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        DiscoveryStrategyConfig discoveryStrategyConfig1 = new DiscoveryStrategyConfig(TEST_CLASS_NAME + "1");
        DiscoveryStrategyConfig discoveryStrategyConfig2 = new DiscoveryStrategyConfig(TEST_CLASS_NAME + "2");
        discoveryConfig.setDiscoveryStrategyConfigs(List.of(discoveryStrategyConfig1, discoveryStrategyConfig2));
        discoveryConfig.setNodeFilterClass(TEST_CLASS_NAME + "Filter");
        expected.setDiscoveryConfig(discoveryConfig);

        WanBatchPublisherConfigHolder holder = transformer.toHolder(expected);
        WanBatchPublisherConfig actual = transformer.toConfig(holder);

        assertWanBatchPublisherEqual(expected, actual);
    }

    @Test
    public void testWanBatchPublisherConfig_rejectsEnabledCloudDiscovery() {
        // Official Hazelcast clients still send the AWS/GCP/Azure/Eureka sections that Hecaton removed (D9)
        WanBatchPublisherConfigHolder h = transformer.toHolder(new WanBatchPublisherConfig());
        WanBatchPublisherConfigHolder withAws = new WanBatchPublisherConfigHolder(h.getPublisherId(), h.getClassName(),
                h.getImplementation(), h.getProperties(), h.getClusterName(), h.isSnapshotEnabled(),
                h.getInitialPublisherState(), h.getQueueCapacity(), h.getBatchSize(), h.getBatchMaxDelayMillis(),
                h.getResponseTimeoutMillis(), h.getQueueFullBehavior(), h.getAcknowledgeType(), h.getDiscoveryPeriodSeconds(),
                h.getMaxTargetEndpoints(), h.getMaxConcurrentInvocations(), h.isUseEndpointPrivateAddress(),
                h.getIdleMinParkNs(), h.getIdleMaxParkNs(), h.getTargetEndpoints(),
                new RemovedDiscoveryConfigHolder("aws", true, false, Map.of()), h.getGcpConfig(), h.getAzureConfig(),
                h.getKubernetesConfig(), h.getEurekaConfig(), h.getDiscoveryConfig(), h.getSyncConfig(), h.getEndpoint());

        assertThatThrownBy(() -> transformer.toConfig(withAws))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Cloud-provider discovery is not supported in Hecaton");
        // disabled sections, as Hecaton clients send them, are accepted
        assertEquals(new WanBatchPublisherConfig().getClusterName(), transformer.toConfig(h).getClusterName());
    }
}
