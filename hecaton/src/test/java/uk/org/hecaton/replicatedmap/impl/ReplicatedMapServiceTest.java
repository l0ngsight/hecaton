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

package uk.org.hecaton.replicatedmap.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.replicatedmap.LocalReplicatedMapStats;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.replicatedmap.impl.iterator.ReplicatedMapIterationService.ITERATOR_CLEANUP_TIMEOUT_MILLIS;
import static uk.org.hecaton.replicatedmap.impl.iterator.ReplicatedMapIterationService.ITERATOR_CLEANUP_PERIOD_SECONDS;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapServiceTest extends HecatonTestSupport {

    private NodeEngine nodeEngine;
    private HecatonInstance hecatonInstance;

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        // make cleanup faster
        config.getProperties().setProperty(ITERATOR_CLEANUP_TIMEOUT_MILLIS.getName(), "2000");
        config.getProperties().setProperty(ITERATOR_CLEANUP_PERIOD_SECONDS.getName(), "10");
        return config;
    }

    @Before
    public void setUp() {
        hecatonInstance = createHecatonInstance();
        nodeEngine = getNodeEngineImpl(hecatonInstance);
    }

    @Test
    public void testShutdown() {
        ReplicatedMapService service = new ReplicatedMapService(nodeEngine);
        service.init(nodeEngine, null);

        service.shutdown(true);
    }

    @Test
    public void testShutdown_withoutInit() {
        ReplicatedMapService service = new ReplicatedMapService(nodeEngine);

        service.shutdown(true);
    }

    @Test
    public void testGetLocalReplicatedMapStatsNoObjectGenerationIfDisabledStats() {
        String name = randomMapName();
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
        replicatedMapConfig.setName(name);
        replicatedMapConfig.setStatisticsEnabled(false);
        nodeEngine.getConfig().addReplicatedMapConfig(replicatedMapConfig);
        ReplicatedMapService service = new ReplicatedMapService(nodeEngine);

        LocalReplicatedMapStats stats = service.getLocalReplicatedMapStats(name);
        LocalReplicatedMapStats stats2 = service.getLocalReplicatedMapStats(name);
        LocalReplicatedMapStats stats3 = service.getLocalReplicatedMapStats(name);
        assertSame(stats, stats2);
        assertSame(stats2, stats3);
    }

    @Test
    public void testCleanups_StaleIterators() {
        String name = randomMapName();
        for (int i = 0; i < 10000; i++) {
            hecatonInstance.getReplicatedMap(name).put(i, i);
        }
        ReplicatedMapService service = getNodeEngineImpl(hecatonInstance).getService(ReplicatedMapService.SERVICE_NAME);
        service.getIterationService().createIterator(name, 0, UuidUtil.newUnsecureUUID());
        assertThat(service.getIterationService().getIteratorManager().getKeySet()).hasSize(1);
        assertTrueEventually(() -> assertThat(service.getIterationService().getIteratorManager().getKeySet()).isEmpty());
    }
}
