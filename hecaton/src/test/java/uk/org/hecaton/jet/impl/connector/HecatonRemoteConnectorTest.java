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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.cache.EventJournalCacheEvent;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.core.processor.SourceProcessors;
import uk.org.hecaton.jet.impl.util.ImdgUtil;
import uk.org.hecaton.map.EventJournalMapEvent;
import uk.org.hecaton.projection.Projections;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static uk.org.hecaton.function.FunctionEx.identity;
import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.EventTimePolicy.eventTimePolicy;
import static uk.org.hecaton.jet.core.WatermarkPolicy.limitingLag;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeListP;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeRemoteCacheP;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeMapP;
import static uk.org.hecaton.jet.core.processor.SourceProcessors.readCacheP;
import static uk.org.hecaton.jet.core.processor.SourceProcessors.readMapP;
import static uk.org.hecaton.jet.core.processor.SourceProcessors.readRemoteCacheP;
import static uk.org.hecaton.jet.core.processor.SourceProcessors.streamRemoteCacheP;
import static uk.org.hecaton.jet.core.processor.SourceProcessors.streamRemoteMapP;
import static uk.org.hecaton.jet.impl.connector.HecatonReaders.readRemoteMapSupplier;
import static uk.org.hecaton.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("StaticVariableName")
public class HecatonRemoteConnectorTest extends JetTestSupport {

    private static final int ITEM_COUNT = 20;
    private static String SOURCE_NAME = randomName() + "-source";
    private static String SINK_NAME = randomName() + "-sink";

    private static HecatonInstance localHz;
    private static HecatonInstance remoteHz;
    private static ClientConfig clientConfig;
    private static final TestHecatonFactory factory = new TestHecatonFactory();

    @BeforeClass
    public static void setUp() {
        Config config = smallInstanceConfig();
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));

        localHz = factory.newHecatonInstance(config);
        HecatonInstance localHz2 = factory.newHecatonInstance(config);

        Config remoteClusterConfig = smallInstanceConfig();
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig().setName("*");
        cacheConfig.getEventJournalConfig().setEnabled(true);
        remoteClusterConfig.addCacheConfig(cacheConfig);
        remoteClusterConfig.setClusterName(randomName());
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("*").getEventJournalConfig().setEnabled(true);
        remoteClusterConfig.addMapConfig(mapConfig);
        remoteHz = Hecaton.newHecatonInstance(remoteClusterConfig);
        HecatonInstance hz2 = Hecaton.newHecatonInstance(remoteClusterConfig);

        clientConfig = new ClientConfig();
        clientConfig.setClusterName(remoteClusterConfig.getClusterName());
        Address address = remoteHz.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());

        // workaround for `cache is not created` exception, create cache locally on all nodes
        hz2.getCacheManager().getCache(SOURCE_NAME);
        hz2.getCacheManager().getCache(SINK_NAME);
        localHz2.getCacheManager().getCache(SOURCE_NAME);
        localHz2.getCacheManager().getCache(SINK_NAME);
    }

    @AfterClass
    public static void teardown() {
        HecatonClient.shutdownAll();
        HecatonInstanceFactory.terminateAll();
        factory.terminateAll();
    }

    @Before
    public void setup() {
        destroyObjects(localHz);
        destroyObjects(remoteHz);
        SOURCE_NAME = randomName() + "-source";
        SINK_NAME = randomName() + "-sink";
    }

    public void destroyObjects(HecatonInstance hz) {
        hz.getDistributedObjects().forEach(DistributedObject::destroy);
    }

    @Test
    public void when_readRemoteMap_withNativePredicateAndProjection() {
        populateMap(remoteHz.getMap(SOURCE_NAME));

        DAG dag = new DAG();

        var config = new RemoteMapSourceConfiguration<>(
                SOURCE_NAME,
                null,
                clientConfig,
                Predicates.greaterThan("this", "0"),
                Projections.singleAttribute("value")
        );
        Vertex source = dag.newVertex("source",
                readRemoteMapSupplier(config)
        ).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IList<Object> list = localHz.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_readRemoteMap_withPredicateAndFunction() {
        populateMap(remoteHz.getMap(SOURCE_NAME));

        DAG dag = new DAG();

        var config = new RemoteMapSourceConfiguration<>(
                SOURCE_NAME,
                null,
                clientConfig,
                e -> !e.getKey().equals(0),
                Entry::getValue
        );

        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteMapSupplier(config)).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        executeAndWait(dag);
        IList<Object> list = localHz.getList(SINK_NAME);
        assertEquals(ITEM_COUNT - 1, list.size());
        assertFalse(list.contains(0));
        assertTrue(list.contains(1));
    }

    @Test
    public void when_writeRemoteMap() {
        populateMap(localHz.getMap(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readMapP(SOURCE_NAME));

        MapSinkConfiguration<Integer, Integer, Integer> sinkConfig = new MapSinkConfiguration<>(SINK_NAME);
        sinkConfig.setClientXml(ImdgUtil.asXmlString(clientConfig));
        sinkConfig.setToKeyFn(identity());
        sinkConfig.setToValueFn(identity());

        Vertex consumer = dag.newVertex(SINK_NAME, writeMapP(sinkConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, remoteHz.getMap(SINK_NAME).size());
    }

    @Test
    public void when_readRemoteCache() {
        populateCache(remoteHz.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, readRemoteCacheP(SOURCE_NAME, clientConfig));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, localHz.getList(SINK_NAME).size());
    }

    @Test
    public void when_writeRemoteCache() {
        populateCache(localHz.getCacheManager().getCache(SOURCE_NAME));

        DAG dag = new DAG();
        Vertex producer = dag.newVertex(SOURCE_NAME, readCacheP(SOURCE_NAME));
        Vertex consumer = dag.newVertex(SINK_NAME, writeRemoteCacheP(SINK_NAME, clientConfig));
        dag.edge(between(producer, consumer));

        executeAndWait(dag);
        assertEquals(ITEM_COUNT, remoteHz.getCacheManager().getCache(SINK_NAME).size());
    }

    @Test
    public void when_streamRemoteMap() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, streamRemoteMapP(SOURCE_NAME, clientConfig, START_FROM_OLDEST,
                eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = localHz.getJet().newJob(dag);

        populateMap(remoteHz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, localHz.getList(SINK_NAME));
        job.cancel();
    }

    @Test
    public void when_streamRemoteMap_withPredicateAndProjection() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, SourceProcessors.<Integer, Integer, Integer>streamRemoteMapP(
                SOURCE_NAME, clientConfig, event -> event.getKey() != 0, EventJournalMapEvent::getKey,
                START_FROM_OLDEST,
                eventTimePolicy(i -> i, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = localHz.getJet().newJob(dag);

        populateMap(remoteHz.getMap(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, localHz.getList(SINK_NAME));
        assertFalse(localHz.getList(SINK_NAME).contains(0));
        assertTrue(localHz.getList(SINK_NAME).contains(1));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME,
                streamRemoteCacheP(SOURCE_NAME, clientConfig, START_FROM_OLDEST,
                        eventTimePolicy(Entry<Integer, Integer>::getValue, limitingLag(0), 1, 0, 10_000))
        ).localParallelism(4);
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME)).localParallelism(1);
        dag.edge(between(source, sink));

        Job job = localHz.getJet().newJob(dag);

        populateCache(remoteHz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT, localHz.getList(SINK_NAME));
        job.cancel();
    }

    @Test
    public void when_streamRemoteCache_withPredicateAndProjection() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex(SOURCE_NAME, SourceProcessors.<Integer, Integer, Integer>streamRemoteCacheP(
                SOURCE_NAME, clientConfig, event -> !event.getKey().equals(0), EventJournalCacheEvent::getKey,
                START_FROM_OLDEST,
                eventTimePolicy(i -> i, limitingLag(0), 1, 0, 10_000)));
        Vertex sink = dag.newVertex(SINK_NAME, writeListP(SINK_NAME));
        dag.edge(between(source, sink));

        Job job = localHz.getJet().newJob(dag);

        populateCache(remoteHz.getCacheManager().getCache(SOURCE_NAME));

        assertSizeEventually(ITEM_COUNT - 1, localHz.getList(SINK_NAME));
        assertFalse(localHz.getList(SINK_NAME).contains(0));
        assertTrue(localHz.getList(SINK_NAME).contains(1));
        job.cancel();
    }

    private void executeAndWait(DAG dag) {
        assertCompletesEventually(localHz.getJet().newJob(dag).getFuture());
    }


    private static void populateMap(Map<Object, Object> map) {
        map.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(toMap(m -> m, m -> m)));
    }

    private static void populateCache(ICache<Object, Object> cache) {
        cache.putAll(IntStream.range(0, ITEM_COUNT).boxed().collect(toMap(m -> m, m -> m)));
    }
}
