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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.config.AbstractWanPublisherConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.WanCustomPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.WanEvent;
import uk.org.hecaton.wan.WanMigrationAwarePublisher;
import uk.org.hecaton.wan.WanPublisher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for custom WAN publisher implementation migration support.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanPublisherMigrationTest extends HecatonTestSupport {

    @Parameter
    public Boolean failMigrations;

    @Parameters(name = "failMigrations:{0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    private TestHecatonInstanceFactory factory;

    @Before
    public void setUp() {
        factory = createHecatonInstanceFactory(2);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testMigration() {
        HecatonInstance[] instances = factory.newInstances(getConfig());
        HecatonInstance member1 = instances[0];
        HecatonInstance member2 = instances[1];

        IMap<Object, Object> map = member1.getMap("dummyMap");
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        //Find local partitions of this member
        final long partitionsToMigrate = Arrays.stream(getPartitionService(member1).getPartitions())
                .filter(IPartition::isLocal)
                .count();

        member1.shutdown();
        assertClusterSizeEventually(1, member2);
        MigrationCountingWanPublisher publisher = getPublisher(member2);

        if (!failMigrations) {
            assertEquals(exceptionMsg("migrationStart", publisher),
                    partitionsToMigrate, publisher.migrationStart.intValue());
            // it may happen that we have additional partition anti-entropy operations
            // that call process but don't show up as migration operations
            assertTrue("Expected at least " + partitionsToMigrate
                            + " migration operations to be processed but was " + publisher,
                    publisher.migrationProcess.intValue() >= partitionsToMigrate);

            //onMigrationCommit() method of the listener is called asynchronously. So assert eventually
            assertTrueEventually(() -> assertEquals(exceptionMsg("migrationCommit", publisher),
                    partitionsToMigrate,
                    publisher.migrationCommit.intValue())
            );
        } else {
            assertEquals(exceptionMsg("migrationStart", publisher),
                    partitionsToMigrate + 1, publisher.migrationStart.intValue());
            // it may happen that we have additional partition anti-entropy operations
            // that call process but don't show up as migration operations
            assertTrue("Expected at least " + partitionsToMigrate
                            + " migration operations to be processed but was " + publisher,
                    publisher.migrationProcess.intValue() >= partitionsToMigrate);

            //onMigrationCommit() method of the listener is called asynchronously. So assert eventually
            assertTrueEventually(() -> assertEquals(exceptionMsg("migrationCommit", publisher),
                    partitionsToMigrate, publisher.migrationCommit.intValue())
            );

            //onMigrationRollback() method of the listener is called asynchronously. So assert eventually
            assertTrueEventually(() -> assertEquals(exceptionMsg("migrationRollback", publisher),
                    1,
                    publisher.migrationRollback.intValue())
            );
        }
    }

    @Nonnull
    private static String exceptionMsg(String counterName, MigrationCountingWanPublisher publisher) {
        return "not expected " + counterName + " count (" + publisher.toString() + ")";
    }

    @Override
    protected Config getConfig() {
        WanCustomPublisherConfig publisherConfig = new WanCustomPublisherConfig()
                .setPublisherId("dummyPublisherId")
                .setClassName(MigrationCountingWanPublisher.class.getName());
        publisherConfig.getProperties().put("failMigrations", failMigrations);

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("dummyWan")
                .addCustomPublisherConfig(publisherConfig);

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName("dummyWan")
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig("default")
                .setWanReplicationRef(wanRef);

        return smallInstanceConfig()
                .addWanReplicationConfig(wanReplicationConfig)
                .addMapConfig(mapConfig);
    }

    private MigrationCountingWanPublisher getPublisher(HecatonInstance instance) {
        WanReplicationService service = getNodeEngineImpl(instance).getWanReplicationService();
        DelegatingWanScheme delegate = service.getWanReplicationPublishers("dummyWan");
        return (MigrationCountingWanPublisher) delegate.getPublishers().iterator().next();
    }

    public static class MigrationCountingWanPublisher implements
            WanPublisher, WanMigrationAwarePublisher<Map<String, String>> {
        private final AtomicBoolean failMigration = new AtomicBoolean();

        final AtomicLong migrationStart = new AtomicLong();
        final AtomicLong migrationCommit = new AtomicLong();
        final AtomicLong migrationRollback = new AtomicLong();
        final AtomicLong migrationProcess = new AtomicLong();

        @Override
        public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig publisherConfig) {
            Boolean failMigrations = (Boolean) publisherConfig.getProperties().get("failMigrations");
            failMigration.set(failMigrations);
        }

        @Override
        public void shutdown() {

        }

        @Override
        public void doPrepublicationChecks() {

        }

        @Override
        public void publishReplicationEvent(WanEvent eventObject) {

        }

        @Override
        public void publishReplicationEventBackup(WanEvent eventObject) {

        }

        @Override
        public void onMigrationStart(PartitionMigrationEvent event) {
            migrationStart.incrementAndGet();
            if (failMigration.compareAndSet(true, false)) {
                throw new RuntimeException("Intentionally failing migration");
            }
        }

        @Override
        public void onMigrationCommit(PartitionMigrationEvent event) {
            migrationCommit.incrementAndGet();
        }

        @Override
        public void onMigrationRollback(PartitionMigrationEvent event) {
            migrationRollback.incrementAndGet();
        }

        @Override
        public Map<String, String> prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                                                        Collection<ServiceNamespace> collection) {
            Map<String, String> container = new HashMap<>();
            container.put("test", "value");
            return container;
        }

        @Override
        public void processEventContainerReplicationData(int partitionId, Map<String, String> eventContainer) {
            assertNotNull(eventContainer);
            assertEquals(1, eventContainer.size());
            assertEquals("value", eventContainer.get("test"));
            migrationProcess.incrementAndGet();
        }

        @Override
        public void collectAllServiceNamespaces(PartitionReplicationEvent event,
                                                Set<ServiceNamespace> set) {
            set.add(new DistributedObjectNamespace(MapService.SERVICE_NAME, "testMap"));
        }

        @Override
        public String toString() {
            return "MigrationCountingWanPublisher{"
                    + "failMigration=" + failMigration
                    + ", migrationStart=" + migrationStart
                    + ", migrationCommit=" + migrationCommit
                    + ", migrationRollback=" + migrationRollback
                    + ", migrationProcess=" + migrationProcess
                    + '}';
        }
    }
}
