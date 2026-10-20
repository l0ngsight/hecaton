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
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec;
import uk.org.hecaton.config.DataPersistenceConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.PartitioningAttributeConfig;
import uk.org.hecaton.config.TieredStoreConfig;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class AddMapConfigMessageTaskTest extends ConfigMessageTaskTest<AddMapConfigMessageTask> {
    @Test
    public void doNotThrowException_whenNullValuesProvidedForNullableFields() {
        MapConfig mapConfig = new MapConfig("my-map");
        ClientMessage addMapConfigClientMessage = DynamicConfigAddMapConfigCodec.encodeRequest(
                mapConfig.getName(),
                mapConfig.getBackupCount(),
                mapConfig.getAsyncBackupCount(),
                mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(),
                null,
                mapConfig.isReadBackupData(),
                mapConfig.getCacheDeserializedValues().name(),
                mapConfig.getMergePolicyConfig().getPolicy(),
                mapConfig.getMergePolicyConfig().getBatchSize(),
                mapConfig.getInMemoryFormat().name(),
                null,
                null,
                mapConfig.isStatisticsEnabled(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                mapConfig.getMetadataPolicy().getId(),
                mapConfig.isPerEntryStatsEnabled(),
                mapConfig.getDataPersistenceConfig(),
                mapConfig.getTieredStoreConfig(),
                null,
                mapConfig.getUserCodeNamespace()
        );
        AddMapConfigMessageTask addMapConfigMessageTask = createMessageTask(addMapConfigClientMessage);
        addMapConfigMessageTask.run();
        MapConfig transmittedMapConfig = (MapConfig) addMapConfigMessageTask.getConfig();
        assertEquals(mapConfig, transmittedMapConfig);
    }

    @Test
    public void testDataPersistenceAndTieredStoreConfigTransmittedCorrectly() {
        MapConfig mapConfig = new MapConfig("my-map");
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        dataPersistenceConfig.setFsync(true);
        mapConfig.setDataPersistenceConfig(dataPersistenceConfig);
        TieredStoreConfig tieredStoreConfig = mapConfig.getTieredStoreConfig();
        tieredStoreConfig.setEnabled(true);
        tieredStoreConfig.getMemoryTierConfig().setCapacity(Capacity.of(1L, MemoryUnit.GIGABYTES));
        tieredStoreConfig.getDiskTierConfig().setEnabled(true).setDeviceName("null-device");

        ClientMessage addMapConfigClientMessage = DynamicConfigAddMapConfigCodec.encodeRequest(
                mapConfig.getName(),
                mapConfig.getBackupCount(),
                mapConfig.getAsyncBackupCount(),
                mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(),
                null,
                mapConfig.isReadBackupData(),
                mapConfig.getCacheDeserializedValues().name(),
                mapConfig.getMergePolicyConfig().getPolicy(),
                mapConfig.getMergePolicyConfig().getBatchSize(),
                mapConfig.getInMemoryFormat().name(),
                null,
                null,
                mapConfig.isStatisticsEnabled(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                mapConfig.getMetadataPolicy().getId(),
                mapConfig.isPerEntryStatsEnabled(),
                mapConfig.getDataPersistenceConfig(),
                mapConfig.getTieredStoreConfig(),
                null,
                mapConfig.getUserCodeNamespace()
        );
        AddMapConfigMessageTask addMapConfigMessageTask = createMessageTask(addMapConfigClientMessage);
        addMapConfigMessageTask.run();
        MapConfig transmittedMapConfig = (MapConfig) addMapConfigMessageTask.getConfig();
        assertEquals(mapConfig, transmittedMapConfig);
    }

    @Test
    public void testPartitioningAttributeConfigsTransmittedCorrectly() {
        MapConfig mapConfig = new MapConfig("my-map");
        mapConfig.setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("attr1"),
                new PartitioningAttributeConfig("attr2")
        ));

        ClientMessage addMapConfigClientMessage = DynamicConfigAddMapConfigCodec.encodeRequest(
                mapConfig.getName(),
                mapConfig.getBackupCount(),
                mapConfig.getAsyncBackupCount(),
                mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(),
                null,
                mapConfig.isReadBackupData(),
                mapConfig.getCacheDeserializedValues().name(),
                mapConfig.getMergePolicyConfig().getPolicy(),
                mapConfig.getMergePolicyConfig().getBatchSize(),
                mapConfig.getInMemoryFormat().name(),
                null,
                null,
                mapConfig.isStatisticsEnabled(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                mapConfig.getMetadataPolicy().getId(),
                mapConfig.isPerEntryStatsEnabled(),
                mapConfig.getDataPersistenceConfig(),
                mapConfig.getTieredStoreConfig(),
                mapConfig.getPartitioningAttributeConfigs(),
                mapConfig.getUserCodeNamespace()
        );
        AddMapConfigMessageTask addMapConfigMessageTask = createMessageTask(addMapConfigClientMessage);
        addMapConfigMessageTask.run();
        MapConfig transmittedMapConfig = (MapConfig) addMapConfigMessageTask.getConfig();
        assertEquals(mapConfig, transmittedMapConfig);
    }

    @Override
    protected AddMapConfigMessageTask createMessageTask(ClientMessage clientMessage) {
        return new AddMapConfigMessageTask(clientMessage, logger, mockNodeEngine, mock(), mockClientEngine, mockConnection,
                mockNodeExtension, mock(), config, mock());
    }
}
