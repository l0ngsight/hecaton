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

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec;
import uk.org.hecaton.client.impl.protocol.task.management.AddWanBatchPublisherConfigMessageTask;
import uk.org.hecaton.config.ConsistencyCheckStrategy;
import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanQueueFullBehavior;
import uk.org.hecaton.config.WanReplicationConfig;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AddWanBatchPublisherConfigMessageTaskTest extends ConfigMessageTaskTest<AddWanBatchPublisherConfigMessageTask> {
    @Test
    public void testWanConfig() {
        ArgumentCaptor<WanReplicationConfig> wanCaptor = ArgumentCaptor.forClass(WanReplicationConfig.class);
        ClientMessage message = MCAddWanBatchPublisherConfigCodec.encodeRequest(
                "wan-config-name",
                "targetCluster",
                "publisherId",
                "192.168.19.20:5701",
                500,
                10,
                1000,
                1000,
                WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.getId(),
                WanQueueFullBehavior.THROW_EXCEPTION.getId(),
                ConsistencyCheckStrategy.MERKLE_TREES.getId()
        );
        AddWanBatchPublisherConfigMessageTask task = createMessageTask(message);
        task.run();

        verify(mockNodeEngine.getWanReplicationService()).addWanReplicationConfig(wanCaptor.capture());
        WanReplicationConfig wanReplicationConfig = wanCaptor.getValue();

        assertEquals("wan-config-name", wanReplicationConfig.getName());
        WanBatchPublisherConfig publisherConfig = wanReplicationConfig.getBatchPublisherConfigs().get(0);
        assertEquals("publisherId", publisherConfig.getPublisherId());
        assertEquals("targetCluster", publisherConfig.getClusterName());
        assertEquals("192.168.19.20:5701", publisherConfig.getTargetEndpoints());
        assertEquals(500, publisherConfig.getQueueCapacity());
        assertEquals(10, publisherConfig.getBatchSize());
        assertEquals(1000, publisherConfig.getBatchMaxDelayMillis());
        assertEquals(1000, publisherConfig.getResponseTimeoutMillis());
        assertEquals(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE, publisherConfig.getAcknowledgeType());
        assertEquals(WanQueueFullBehavior.THROW_EXCEPTION, publisherConfig.getQueueFullBehavior());
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, publisherConfig.getSyncConfig().getConsistencyCheckStrategy());
    }

    @Override
    protected AddWanBatchPublisherConfigMessageTask createMessageTask(ClientMessage clientMessage) {
        return new AddWanBatchPublisherConfigMessageTask(clientMessage, logger, mockNodeEngine, mock(), mockClientEngine,
                mockConnection, mockNodeExtension, mock(), config, mock());
    }
}
