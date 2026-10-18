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

package uk.org.hecaton.jet.core;

import uk.org.hecaton.client.map.helpers.AMapStore;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.core.JobRestartWithSnapshotTest.SequencesInPartitionsGeneratorP;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.TestUtil.throttle;
import static uk.org.hecaton.jet.core.processor.DiagnosticProcessors.peekOutputP;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeMapP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotFailureTest extends JetTestSupport {

    private static final int LOCAL_PARALLELISM = 4;

    private static volatile boolean storeFailed;

    private HecatonInstance instance1;

    @Before
    public void setup() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(LOCAL_PARALLELISM);

        // force snapshots to fail by adding a failing map store configuration for snapshot data maps
        MapConfig mapConfig = new MapConfig(JobRepository.SNAPSHOT_DATA_MAP_PREFIX + '*');
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new FailingMapStore());
        config.addMapConfig(mapConfig);

        HecatonInstance[] instances = createHecatonInstances(config, 2);
        instance1 = instances[0];
    }

    @Test
    public void when_snapshotFails_then_jobShouldNotFail() {
        storeFailed = false;
        int numPartitions = 2;
        int numElements = 10;
        IMap<Object, Object> results = instance1.getMap("results");

        DAG dag = new DAG();
        SupplierEx<Processor> sup = () -> new SequencesInPartitionsGeneratorP(numPartitions, numElements, false);
        Vertex generator = dag.newVertex("generator", peekOutputP(throttle(sup, 2)))
                              .localParallelism(1);
        Vertex writeMap = dag.newVertex("writeMap", writeMapP(results.getName())).localParallelism(1);
        dag.edge(between(generator, writeMap));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(100);

        Job job = instance1.getJet().newJob(dag, config);
        job.join();

        assertEquals("numPartitions", numPartitions, results.size());
        assertEquals("offset partition 0", numElements - 1, results.get(0));
        assertEquals("offset partition 1", numElements - 1, results.get(1));
        assertTrue("no failure occurred in store", storeFailed);
    }

    private static class FailingMapStore extends AMapStore implements Serializable {
        @Override
        public void store(Object o, Object o2) {
            storeFailed = true;
            throw new UnsupportedOperationException();
        }
    }
}
