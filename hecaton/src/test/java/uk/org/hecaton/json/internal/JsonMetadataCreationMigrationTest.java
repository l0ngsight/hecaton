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

package uk.org.hecaton.json.internal;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MetadataPolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.query.impl.JsonMetadata;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getBackupInstance;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class JsonMetadataCreationMigrationTest extends HecatonTestSupport {

    protected static final int ENTRY_COUNT = 1000;

    protected TestHecatonInstanceFactory factory;
    protected final int NODE_COUNT = 5;

    @Before
    public void setup() {
        factory = createHecatonInstanceFactory(NODE_COUNT);
    }

    @Test
    public void testMetadataIsCreatedWhenRecordsAreMigrated() {
        HecatonInstance instance = factory.newHecatonInstance(getConfig());

        final IMap<HecatonJsonValue, HecatonJsonValue> map = instance.getMap(randomMapName());
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), createJsonValue("value", i));
        }

        for (int i = 1; i < NODE_COUNT; i++) {
            factory.newHecatonInstance(getConfig());
        }

        waitAllForSafeState(factory.getAllHecatonInstances());
        warmUpPartitions(factory.getAllHecatonInstances());

        assertMetadataCreatedEventually(map.getName());
    }


    protected void assertMetadataCreatedEventually(final String mapName) {
        assertTrueEventually(() -> assertMetadataCreated(mapName, NODE_COUNT));
    }

    protected void assertMetadataCreated(String mapName, int replicaCount) {
        for (int i = 0; i < replicaCount; i++) {
            for (int j = 0; j < ENTRY_COUNT; j++) {
                assertMetadata(getMetadata(mapName, createJsonValue("key", j), i));
            }
        }
    }

    protected void assertMetadata(JsonMetadata metadata) {
        assertNotNull(metadata);
        JsonSchemaNode keyNode = (JsonSchemaNode) metadata.getKeyMetadata();
        assertNotNull(keyNode);
        assertFalse(keyNode.isTerminal());
        JsonSchemaNode childNode = ((JsonSchemaStructNode) keyNode).getChild(0).getValue();
        assertTrue(childNode.isTerminal());

        JsonSchemaNode valueNode = (JsonSchemaNode) metadata.getValueMetadata();
        assertNotNull(valueNode);
        assertFalse(valueNode.isTerminal());
        JsonSchemaNode valueChildNode = ((JsonSchemaStructNode) valueNode).getChild(0).getValue();
        assertTrue(valueChildNode.isTerminal());
    }

    protected JsonMetadata getMetadata(String mapName, Object key, int replicaIndex) {
        HecatonInstance[] instances = factory.getAllHecatonInstances().toArray(new HecatonInstance[] { null });
        HecatonInstance instance = factory.getAllHecatonInstances().iterator().next();
        InternalSerializationService serializationService = getSerializationService(instance);
        Data keyData = serializationService.toData(key);
        int partitionId = getPartitionService(instance).getPartitionId(key);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(getBackupInstance(instances, partitionId, replicaIndex));
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        RecordStore recordStore = mapService.getMapServiceContext().getPartitionContainer(partitionId).getRecordStore(mapName);
        return recordStore.getOrCreateMetadataStore().get(keyData);
    }

    protected Config getConfig() {
        Config config = new Config();
        config.getMapConfig("default")
                .setBackupCount(NODE_COUNT - 1)
                .setAsyncBackupCount(0)
                .setMetadataPolicy(MetadataPolicy.CREATE_ON_UPDATE);
        return config;
    }

    protected static HecatonJsonValue createJsonValue(String type, int innerValue) {
        return new HecatonJsonValue(Json.object()
                .add("type", type)
                .add("value", innerValue)
                .toString());
    }
}
