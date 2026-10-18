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

package uk.org.hecaton.jet.pipeline;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.dataconnection.HecatonDataConnection;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static uk.org.hecaton.jet.Util.entry;
import static uk.org.hecaton.jet.pipeline.DataConnectionRef.dataConnectionRef;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class})
public class SinksRemoteMapWithUpdatingTest extends PipelineTestSupport {

    private static HecatonInstance remoteHz;
    private static ClientConfig clientConfig;

    private static final String HC_CLIENT_DATA_CONNECTION_NAME = "hzclientexternalref";

    @BeforeClass
    public static void setUp() throws IOException {
        // Create remote cluster
        String clusterName = "test123";

        Config remoteClusterConfig = new Config();
        remoteClusterConfig.setClusterName(clusterName);
        remoteClusterConfig.addCacheConfig(new CacheSimpleConfig().setName("*"));
        remoteHz = createRemoteCluster(remoteClusterConfig, 2).get(0);

        clientConfig = getClientConfigForRemoteCluster(remoteHz);

        // Create local cluster
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig(HC_CLIENT_DATA_CONNECTION_NAME);
        dataConnectionConfig.setType("HZ");

        // Read XML and set as DataConnectionConfig
        String xmlString = readLocalClusterConfig("hecaton-client-test-external.xml", clusterName, remoteHz);
        dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML, xmlString);

        for (HecatonInstance hecatonInstance : allHecatonInstances()) {
            Config hecatonInstanceConfig = hecatonInstance.getConfig();
            hecatonInstanceConfig.addDataConnectionConfig(dataConnectionConfig);
        }
    }

    @AfterClass
    public static void afterClass() {
        HecatonInstanceFactory.terminateAll();
    }

    @Test
    public void remoteMapWithUpdating() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                clientConfig,
                Entry::getKey,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                .map(i -> entry(String.valueOf(i), i + 10))
                .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithUpdating_withExternalConfig() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                dataConnectionRef(HC_CLIENT_DATA_CONNECTION_NAME),
                Entry::getKey,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                .map(i -> entry(String.valueOf(i), i + 10))
                .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithUpdating_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(srcName,
                clientConfig,
                (value, item) -> null);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void remoteMapWithUpdating_when_functionReturnsNull_then_keyIsRemoved_withExternalConfig() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(srcName,
                dataConnectionRef(HC_CLIENT_DATA_CONNECTION_NAME),
                (value, item) -> null);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void remoteMapWithUpdating_when_itemDataSerializable() {
        // Given
        IMap<String, DataSerializableObject> sourceMap = remoteHz.getMap(srcName);
        List<Integer> input = sequence(itemCount);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        Sink<Entry<String, DataSerializableObject>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                clientConfig,
                (value, item) -> new DataSerializableObject(value.value + item.getValue().value));

        // Then
        p.readFrom(Sources.<String, DataSerializableObject>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, DataSerializableObject>> expected = input
                .stream()
                .map(i -> entry(String.valueOf(i), new DataSerializableObject(i * 2)))
                .collect(toList());
        Set<Entry<String, DataSerializableObject>> actual = sourceMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithUpdating_when_itemDataSerializable_withExternalConfig() {
        // Given
        IMap<String, DataSerializableObject> sourceMap = remoteHz.getMap(srcName);
        List<Integer> input = sequence(itemCount);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        Sink<Entry<String, DataSerializableObject>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                dataConnectionRef(HC_CLIENT_DATA_CONNECTION_NAME),
                (value, item) -> new DataSerializableObject(value.value + item.getValue().value));

        // Then
        p.readFrom(Sources.<String, DataSerializableObject>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, DataSerializableObject>> expected = input
                .stream()
                .map(i -> entry(String.valueOf(i), new DataSerializableObject(i * 2)))
                .collect(toList());
        Set<Entry<String, DataSerializableObject>> actual = sourceMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    private static class DataSerializableObject implements DataSerializable {
        int value;

        DataSerializableObject() {
        }

        DataSerializableObject(int value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DataSerializableObject that = (DataSerializableObject) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
