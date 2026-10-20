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

package uk.org.hecaton.jet.impl.util;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.config.EdgeConfig;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeMapP;
import static uk.org.hecaton.jet.core.processor.SourceProcessors.readMapP;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class ImdgUtilTest extends SimpleTestInClusterSupport {

    private static final String NEAR_CACHED_SERIALIZED_MAP = "nearCachedSerialized";
    private static final String NEAR_CACHED_NON_SERIALIZED_MAP = "nearCachedNonSerialized";

    @BeforeClass
    public static void setupCluster() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        config.getMapConfig(NEAR_CACHED_SERIALIZED_MAP).setNearCacheConfig(
                new NearCacheConfig().setInMemoryFormat(InMemoryFormat.BINARY)
        );
        config.getMapConfig(NEAR_CACHED_NON_SERIALIZED_MAP).setNearCacheConfig(
                new NearCacheConfig().setInMemoryFormat(InMemoryFormat.OBJECT)
        );

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(new NearCacheConfig(NEAR_CACHED_SERIALIZED_MAP)
                .setInMemoryFormat(InMemoryFormat.BINARY));
        clientConfig.addNearCacheConfig(new NearCacheConfig(NEAR_CACHED_NON_SERIALIZED_MAP)
                .setInMemoryFormat(InMemoryFormat.OBJECT));

        initializeWithClient(2, config, clientConfig);
    }

    @Test
    public void test_copyMap() throws Exception {
        logger.info("Populating source map...");
        IMap<Object, Object> srcMap = instance().getMap("src");
        Map<Integer, Integer> testData = IntStream.range(0, 100_000).boxed().collect(toMap(e -> e, e -> e));
        srcMap.putAll(testData);

        logger.info("Copying using job...");
        copyMapUsingJob(instance(), 128, srcMap.getName(), "target").get();
        logger.info("Done copying");

        assertEquals(testData, new HashMap<>(instance().getMap("target")));
    }

    private static CompletableFuture<Void> copyMapUsingJob(HecatonInstance instance, int queueSize,
                                                           String sourceMap, String targetMap) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("readMap(" + sourceMap + ')', readMapP(sourceMap));
        Vertex sink = dag.newVertex("writeMap(" + targetMap + ')', writeMapP(targetMap));
        dag.edge(between(source, sink).setConfig(new EdgeConfig().setQueueSize(queueSize)));
        JobConfig jobConfig = new JobConfig()
                .setName("copy-" + sourceMap + "-to-" + targetMap);
        return instance.getJet().newJob(dag, jobConfig).getFuture();
    }
}
