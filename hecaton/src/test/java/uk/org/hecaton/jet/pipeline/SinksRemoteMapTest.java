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
public class SinksRemoteMapTest extends PipelineTestSupport {

    private static HecatonInstance remoteHz;
    private static ClientConfig clientConfig;

    private static final String HC_CLIENT_DATA_CONNECTION_NAME = "hzclientexternalref";

    @BeforeClass
    public static void setUp() throws IOException {
        // Create remote cluster
        String clusterName = randomName();

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
    public void remoteMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMap(sinkName, clientConfig);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                .map(i -> entry(String.valueOf(i), i))
                .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(sinkName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMap_withExternalConfig() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMap(sinkName,
                dataConnectionRef(HC_CLIENT_DATA_CONNECTION_NAME));

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                .map(i -> entry(String.valueOf(i), i))
                .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(sinkName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

}
