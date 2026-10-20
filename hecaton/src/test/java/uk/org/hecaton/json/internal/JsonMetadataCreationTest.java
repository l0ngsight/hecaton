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
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.config.MetadataPolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapLoader;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.JsonMetadataStore;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.query.impl.JsonMetadata;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.backup.MapBackupAccessor;
import uk.org.hecaton.test.backup.TestBackupUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.test.Accessors.getBackupInstance;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonMetadataCreationTest extends HecatonTestSupport {

    private static final int ENTRY_COUNT = 1000;
    private static final int NODE_COUNT = 3;
    private static final int PARTITION_COUNT = 10;

    protected TestHecatonInstanceFactory factory;
    protected HecatonInstance[] instances;
    private IMap map;
    private IMap mapWithMapStore;
    private IMap mapWithoutMetadata;

    @Parameterized.Parameters(name = "InMemoryFormat: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT}
        });
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Before
    public void setup() {
        factory = createHecatonInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(getConfig());
        map = instances[0].getMap(randomMapName());
        mapWithoutMetadata = instances[0].getMap("noprocessing" + randomMapName());
        mapWithMapStore = instances[0].getMap("mapStore" + randomName());
    }

    @Test
    public void testPutCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testMetadataIsntCreatedWhenKeyAndValueAreNotJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < NODE_COUNT; i++) {
            for (int j = 0; j < ENTRY_COUNT; j++) {
                assertNull(getMetadata(map.getName(), j, i));
            }
        }
    }

    @Test
    public void testMetadataIsRemoved_whenValueBecomesNonJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, createJsonValue("value", i));
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < NODE_COUNT; i++) {
            for (int j = 0; j < ENTRY_COUNT; j++) {
                assertNull(getMetadata(map.getName(), j, i));
            }
        }
    }

    @Test
    public void testPutDoesNotCreateMetadata_whenMetadataPolicyIsOff() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            mapWithoutMetadata.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataNotCreated(mapWithoutMetadata.getName());
    }

    @Test
    public void testPutCreatesMetadataForJson_whenReplacingExisting() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "somevalue");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutAsyncCreatesMetadataForJson() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putAsync(createJsonValue("key", i), createJsonValue("value", i)).toCompletableFuture().get();
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testTryPutCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.tryPut(createJsonValue("key", i), createJsonValue("value", i), 1, TimeUnit.HOURS);
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutTransientCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putTransient(createJsonValue("key", i), createJsonValue("value", i), 1, TimeUnit.HOURS);
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutIfAbsentCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putIfAbsent(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testPutIfAbsentAsyncCreatesMetadataForJson() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            ((MapProxyImpl) map)
                    .putIfAbsentAsync(createJsonValue("key", i), createJsonValue("value", i)).toCompletableFuture().get();
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testEntryProcessorCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        map.executeOnEntries(new ModifyingEntryProcessor());
        assertMetadataCreatedEventually(map.getName());
    }

    @Test
    public void testReplaceCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.replace(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testReplaceIfSameCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.replace(createJsonValue("key", i), "not-json-value", createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testLoadAllCreatesMetadataForJson() {
        mapWithMapStore.loadAll(false);
        assertMetadataCreatedEventually(mapWithMapStore.getName());
    }

    @Test
    public void testLoadCreatesMetadataForJson() {
        mapWithMapStore.loadAll(false);
        mapWithMapStore.evictAll();

        for (int i = 0; i < ENTRY_COUNT; i++) {
            mapWithMapStore.get(createJsonValue("key", i));
        }
        assertMetadataCreated(mapWithMapStore.getName(), 1);
    }

    @Test
    public void testPutAllCreatesMetadataForJson() {
        Map<HecatonJsonValue, HecatonJsonValue> localMap = new HashMap<>();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            localMap.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        map.putAll(localMap);

        waitAllForSafeState(instances);

        assertMetadataCreated(map.getName());
    }

    @Test
    public void testSetCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.set(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testSetAsyncCreatesMetadataForJson() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.setAsync(createJsonValue("key", i), createJsonValue("value", i)).toCompletableFuture().get();
        }
        assertMetadataCreated(map.getName());
    }

    protected int getPartitionCount() {
        return PARTITION_COUNT;
    }

    protected int getNodeCount() {
        return NODE_COUNT;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(getPartitionCount()));
        config.getMapConfig("default")
                .setBackupCount(getNodeCount() - 1)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(getInMemoryFormat())
                .setMetadataPolicy(MetadataPolicy.CREATE_ON_UPDATE);
        config.getMapConfig("noprocessing*")
                .setMetadataPolicy(MetadataPolicy.OFF)
                .setInMemoryFormat(getInMemoryFormat());
        config.getMapConfig("mapStore*")
                .setInMemoryFormat(getInMemoryFormat())
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(new JsonMapLoader())
                        .setEnabled(true).setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY));
        return config;
    }

    protected JsonMetadata getMetadata(String mapName, Object key, int replicaIndex) {
        HecatonInstance[] instances = factory.getAllHecatonInstances().toArray(new HecatonInstance[]{null});
        HecatonInstance instance = factory.getAllHecatonInstances().iterator().next();
        InternalSerializationService serializationService = getSerializationService(instance);
        Data keyData = serializationService.toData(key);
        int partitionId = getPartitionService(instance).getPartitionId(key);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(getBackupInstance(instances, partitionId, replicaIndex));
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        RecordStore recordStore = mapService.getMapServiceContext().getPartitionContainer(partitionId).getRecordStore(mapName);
        JsonMetadataStore metadataStore = recordStore.getOrCreateMetadataStore();
        return metadataStore.get(keyData);
    }

    private void assertMetadataCreatedEventually(final String mapName) {
        assertTrueEventually(() -> assertMetadataCreated(mapName));
    }

    private void assertMetadataCreated(String mapName) {
        assertMetadataCreated(mapName, NODE_COUNT);
    }

    private void assertMetadataCreated(String mapName, int replicaCount) {
        for (int i = 0; i < replicaCount; i++) {
            MapBackupAccessor mapBackupAccessor = (MapBackupAccessor) TestBackupUtils.newMapAccessor(instances, mapName, i);
            for (int j = 0; j < ENTRY_COUNT; j++) {
                Record record = mapBackupAccessor.getRecord(createJsonValue("key", j));
                assertNotNull(record);
                HecatonJsonValue jsonValue = createJsonValue("key", j);
                JsonMetadata metadata = getMetadata(mapName, jsonValue, i);
                assertMetadata("Replica index=" + i, metadata);
            }
        }
    }

    private void assertMetadataNotCreated(String mapName) {
        assertMetadataNotCreated(mapName, NODE_COUNT);
    }

    private void assertMetadataNotCreated(String mapName, int replicaCount) {
        for (int i = 0; i < replicaCount; i++) {
            MapBackupAccessor mapBackupAccessor = (MapBackupAccessor) TestBackupUtils.newMapAccessor(instances, mapName, i);
            for (int j = 0; j < ENTRY_COUNT; j++) {
                Record record = mapBackupAccessor.getRecord(createJsonValue("key", j));
                assertNotNull(record);
                assertNull(getMetadata(mapName, createJsonValue("key", j), i));
            }
        }
    }

    private void assertMetadata(String msg, JsonMetadata metadata) {
        assertNotNull(msg, metadata);
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

    static class JsonMapLoader implements MapLoader<HecatonJsonValue, HecatonJsonValue> {

        @Override
        public HecatonJsonValue load(HecatonJsonValue key) {
            int value = Json.parse(key.toString()).asObject().get("value").asInt();
            return createJsonValue("value", value);
        }

        @Override
        public Map<HecatonJsonValue, HecatonJsonValue> loadAll(Collection<HecatonJsonValue> keys) {
            Map<HecatonJsonValue, HecatonJsonValue> localMap = new HashMap<>();
            for (HecatonJsonValue key : keys) {
                int value = Json.parse(key.toString()).asObject().get("value").asInt();
                localMap.put(key, createJsonValue("value", value));
            }

            return localMap;
        }

        @Override
        public Iterable<HecatonJsonValue> loadAllKeys() {
            Collection<HecatonJsonValue> localKeys = new ArrayList<>();
            for (int i = 0; i < ENTRY_COUNT; i++) {
                localKeys.add(createJsonValue("key", i));
            }
            return localKeys;
        }
    }

    static HecatonJsonValue createJsonValue(String type, int innerValue) {
        return new HecatonJsonValue(Json.object()
                .add("type", type)
                .add("value", innerValue)
                .toString());
    }

    static class ModifyingEntryProcessor implements EntryProcessor<HecatonJsonValue, Object, Object> {
        @Override
        public Object process(Map.Entry<HecatonJsonValue, Object> entry) {
            HecatonJsonValue key = entry.getKey();
            int value = Json.parse(key.toString()).asObject().get("value").asInt();
            entry.setValue(createJsonValue("value", value));
            return null;
        }
    }

}
