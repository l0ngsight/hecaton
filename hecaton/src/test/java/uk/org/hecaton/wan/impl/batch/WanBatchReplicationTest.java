/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.config.WanQueueFullBehavior;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.metrics.impl.CapturingCollector;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.WanConsumer;
import uk.org.hecaton.wan.WanPublisherState;
import uk.org.hecaton.wan.WanQueueFullException;
import uk.org.hecaton.wan.impl.AddWanConfigResult;
import uk.org.hecaton.wan.impl.WanReplicationService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class WanBatchReplicationTest extends HecatonTestSupport {

    private static final String WAN = "to-b";
    private static final String MAP = "replicated";
    private static final String CACHE = "replicated-cache";
    private static final int ENTRIES = 1000;

    private TestHecatonInstanceFactory factory;

    @Before
    public void setUp() {
        factory = createHecatonInstanceFactory(8);
    }

    @Test
    public void mapChangesAreReplicatedToTheTargetCluster() {
        List<HecatonInstance> target = startCluster("b", 2, null);
        List<HecatonInstance> source = startCluster("a", 2, publisher("b", target));
        IMap<Integer, String> map = source.get(0).getMap(MAP);
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        for (int i = 0; i < ENTRIES; i += 2) {
            map.remove(i);
        }
        map.put(1, "updated");

        IMap<Integer, String> replicated = target.get(1).getMap(MAP);
        assertTrueEventually(() -> {
            assertThat(replicated.size()).isEqualTo(ENTRIES / 2);
            assertThat(replicated.get(1)).isEqualTo("updated");
            assertThat(replicated.get(3)).isEqualTo("value3");
        });
        assertThat(replicated.get(0)).isNull();
        assertTrueEventually(() -> assertThat(outboundQueueSize(source)).isZero());
        // the backups' copies of the queues are emptied too
        assertTrueEventually(() -> assertThat(totalQueueSize(source)).isZero());
    }

    @Test
    public void eventsQueuedOnABackupAreSentWhenTheOwnerFails() {
        List<HecatonInstance> target = startCluster("b", 1, null);
        WanBatchPublisherConfig publisher = publisher("b", target);
        List<HecatonInstance> source = startCluster("a", 3, publisher);
        pause(source);
        IMap<Integer, String> map = source.get(0).getMap(MAP);
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        source.get(1).getLifecycleService().terminate();
        List<HecatonInstance> remaining = List.of(source.get(0), source.get(2));
        waitAllForSafeState(remaining);
        resume(remaining);

        IMap<Integer, String> replicated = target.get(0).getMap(MAP);
        assertTrueEventually(() -> assertThat(replicated.size()).isEqualTo(ENTRIES));
        assertTrueEventually(() -> assertThat(totalQueueSize(remaining)).isZero());
    }

    @Test
    public void pausedPublisherQueuesAndStoppedPublisherDropsEvents() {
        List<HecatonInstance> target = startCluster("b", 1, null);
        List<HecatonInstance> source = startCluster("a", 1, publisher("b", target));
        IMap<Integer, String> map = source.get(0).getMap(MAP);
        IMap<Integer, String> replicated = target.get(0).getMap(MAP);

        pause(source);
        map.put(1, "while paused");
        sleepSeconds(2);
        assertThat(replicated.get(1)).isNull();
        resume(source);
        assertTrueEventually(() -> assertThat(replicated.get(1)).isEqualTo("while paused"));

        wanService(source.get(0)).stop(WAN, "b");
        map.put(2, "while stopped");
        resume(source);
        map.put(3, "after");
        assertTrueEventually(() -> assertThat(replicated.get(3)).isEqualTo("after"));
        assertThat(replicated.get(2)).isNull();
    }

    @Test
    public void fullQueueThrowsOrDiscards() {
        // the target cannot be reached: events stay in the queue
        WanBatchPublisherConfig throwing = new WanBatchPublisherConfig().setClusterName("b")
                .setTargetEndpoints("127.0.0.1:1").setQueueCapacity(10)
                .setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION);
        List<HecatonInstance> source = startCluster("a", 1, throwing);
        IMap<Integer, String> map = source.get(0).getMap(MAP);
        for (int i = 0; i < 10; i++) {
            map.put(i, "value");
        }
        assertThatThrownBy(() -> map.put(10, "value")).isInstanceOf(WanQueueFullException.class);

        wanService(source.get(0)).removeWanEvents(WAN, "b");
        map.put(10, "value");
    }

    @Test
    public void queueFullDiscardsByDefault() {
        WanBatchPublisherConfig discarding = new WanBatchPublisherConfig().setClusterName("b")
                .setTargetEndpoints("127.0.0.1:1").setQueueCapacity(10);
        List<HecatonInstance> source = startCluster("a", 1, discarding);
        IMap<Integer, String> map = source.get(0).getMap(MAP);
        for (int i = 0; i < 20; i++) {
            map.put(i, "value");
        }
        assertThat(outboundQueueSize(source)).isEqualTo(10);
        assertThat(wanService(source.get(0)).getSentEventCounters(WAN, "b", "hz:impl:mapService")
                .getEventCounterMap().get(MAP).getDroppedCount()).isEqualTo(10);
    }

    @Test
    public void activeActiveReplicationConverges() {
        // a's map refers to a WAN replication scheme that is added once b exists
        List<HecatonInstance> a = startCluster("a", 1, null, true);
        List<HecatonInstance> b = startCluster("b", 1, publisher("a", a));
        wanService(a.get(0)).addWanReplicationConfig(new WanReplicationConfig().setName(WAN)
                .addBatchReplicationPublisherConfig(publisher("b", b)));
        a.get(0).getMap(MAP).put(1, "from a");
        b.get(0).getMap(MAP).put(2, "from b");
        assertTrueEventually(() -> {
            assertThat(a.get(0).getMap(MAP).get(2)).isEqualTo("from b");
            assertThat(b.get(0).getMap(MAP).get(1)).isEqualTo("from a");
        });
        // the replicated changes are not replicated back
        sleepSeconds(2);
        assertThat(outboundQueueSize(a)).isZero();
        assertThat(outboundQueueSize(b)).isZero();
    }

    @Test
    public void configAddedAtRuntimeAppliesToAllMembers() {
        List<HecatonInstance> b = startCluster("b", 1, null);
        List<HecatonInstance> c = startCluster("c", 1, null);
        List<HecatonInstance> a = startCluster("a", 2, null, true);
        wanService(a.get(0)).addWanReplicationConfig(new WanReplicationConfig().setName(WAN)
                .addBatchReplicationPublisherConfig(publisher("b", b)));
        // a publisher added to the scheme later
        AddWanConfigResult result = wanService(a.get(1)).addWanReplicationConfig(new WanReplicationConfig().setName(WAN)
                .addBatchReplicationPublisherConfig(publisher("b", b))
                .addBatchReplicationPublisherConfig(publisher("c", c)));
        assertThat(result.getAddedPublisherIds()).containsExactly("c");
        assertThat(result.getIgnoredPublisherIds()).containsExactly("b");
        for (HecatonInstance member : a) {
            assertThat(wanService(member).getPublisherOrFail(WAN, "b")).isNotNull();
            assertThat(wanService(member).getPublisherOrFail(WAN, "c")).isNotNull();
        }

        IMap<Integer, String> map = a.get(1).getMap(MAP);
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        assertTrueEventually(() -> {
            assertThat(b.get(0).getMap(MAP).size()).isEqualTo(ENTRIES);
            assertThat(c.get(0).getMap(MAP).size()).isEqualTo(ENTRIES);
        });
    }

    @Test
    public void acknowledgeOnReceipt() {
        List<HecatonInstance> target = startCluster("b", 1, null);
        List<HecatonInstance> source = startCluster("a", 1,
                publisher("b", target).setAcknowledgeType(WanAcknowledgeType.ACK_ON_RECEIPT));
        IMap<Integer, String> map = source.get(0).getMap(MAP);
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        assertTrueEventually(() -> assertThat(target.get(0).getMap(MAP).size()).isEqualTo(ENTRIES));
    }

    @Test
    public void cacheChangesAreReplicatedToTheTargetCluster() {
        List<HecatonInstance> target = startCluster("b", 2, null);
        List<HecatonInstance> source = startCluster("a", 2, publisher("b", target));
        Cache<Integer, String> targetCache = createServerCachingProvider(target.get(0)).getCacheManager().getCache(CACHE);
        Cache<Integer, String> cache = createServerCachingProvider(source.get(0)).getCacheManager().getCache(CACHE);
        for (int i = 0; i < ENTRIES; i++) {
            cache.put(i, "value" + i);
        }
        for (int i = 0; i < ENTRIES; i += 2) {
            cache.remove(i);
        }
        cache.replace(1, "updated");

        assertTrueEventually(() -> {
            assertThat(targetCache.unwrap(ICache.class).size()).isEqualTo(ENTRIES / 2);
            assertThat(targetCache.get(1)).isEqualTo("updated");
        });
        assertThat(targetCache.get(0)).isNull();
        assertTrueEventually(() -> assertThat(outboundQueueSize(source)).isZero());
    }

    @Test
    public void publisherMetricsAreCollected() {
        List<HecatonInstance> target = startCluster("b", 1, null);
        List<HecatonInstance> source = startCluster("a", 1, publisher("b", target));
        IMap<Integer, String> map = source.get(0).getMap(MAP);
        for (int i = 0; i < 10; i++) {
            map.put(i, "value" + i);
        }
        assertTrueEventually(() -> assertThat(target.get(0).getMap(MAP).size()).isEqualTo(10));
        assertTrueEventually(() -> {
            CapturingCollector collector = new CapturingCollector();
            getNode(source.get(0)).getNodeEngine().getMetricsRegistry().collect(collector);
            assertThat(metric(collector, "totalPublishedEventCount", null)).isEqualTo(10);
            assertThat(metric(collector, "outboundQueueSize", null)).isZero();
            assertThat(metric(collector, "updateCount", MAP)).isEqualTo(10);
        });
    }

    private static long metric(CapturingCollector collector, String name, String map) {
        return collector.captures().entrySet().stream()
                .filter(capture -> "wan".equals(capture.getKey().prefix()) && name.equals(capture.getKey().metric())
                        && WAN.equals(capture.getKey().discriminatorValue())
                        && "b".equals(capture.getKey().tagValue("publisherId"))
                        && (map == null || map.equals(capture.getKey().tagValue("map"))))
                .map(capture -> capture.getValue().singleCapturedValue().longValue())
                .findFirst().orElseThrow(() -> new AssertionError("metric " + name + " not collected"));
    }

    @Test
    public void customConsumerIsCreatedOnStartAndShutDown() {
        Config config = smallInstanceConfig().setClusterName("consumer");
        config.addWanReplicationConfig(new WanReplicationConfig().setName("from-a")
                .setConsumerConfig(new WanConsumerConfig().setClassName(RecordingConsumer.class.getName())));
        HecatonInstance member = factory.newHecatonInstance(config);
        assertThat(RecordingConsumer.INITIALIZED).containsExactly("from-a");
        member.shutdown();
        assertThat(RecordingConsumer.SHUT_DOWN).containsExactly("from-a");
    }

    private WanBatchPublisherConfig publisher(String targetCluster, List<HecatonInstance> target) {
        String endpoints = target.stream()
                .map(member -> member.getCluster().getLocalMember().getAddress().getHost() + ":"
                        + member.getCluster().getLocalMember().getAddress().getPort())
                .collect(Collectors.joining(","));
        return new WanBatchPublisherConfig().setClusterName(targetCluster).setTargetEndpoints(endpoints)
                .setBatchSize(100).setBatchMaxDelayMillis(50);
    }

    private List<HecatonInstance> startCluster(String name, int size, WanBatchPublisherConfig publisher) {
        return startCluster(name, size, publisher, publisher != null);
    }

    private List<HecatonInstance> startCluster(String name, int size, WanBatchPublisherConfig publisher,
                                               boolean replicatedMap) {
        Config config = smallInstanceConfig().setClusterName(name);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        if (publisher != null) {
            config.addWanReplicationConfig(new WanReplicationConfig().setName(WAN)
                    .addBatchReplicationPublisherConfig(publisher));
        }
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig().setName(CACHE);
        if (replicatedMap) {
            cacheConfig.setWanReplicationRef(new WanReplicationRef().setName(WAN)
                    .setMergePolicyClassName(PassThroughMergePolicy.class.getName()));
        }
        config.addCacheConfig(cacheConfig);
        if (replicatedMap) {
            config.addMapConfig(new MapConfig(MAP).setWanReplicationRef(new WanReplicationRef()
                    .setName(WAN).setMergePolicyClassName(PassThroughMergePolicy.class.getName())));
        }
        List<HecatonInstance> members = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            members.add(factory.newHecatonInstance(config));
        }
        assertClusterSizeEventually(size, members);
        return members;
    }

    private static void pause(List<HecatonInstance> members) {
        members.forEach(member -> wanService(member).pause(WAN, "b"));
    }

    private static void resume(List<HecatonInstance> members) {
        members.forEach(member -> wanService(member).resume(WAN, "b"));
    }

    private static int outboundQueueSize(List<HecatonInstance> members) {
        int size = 0;
        for (HecatonInstance member : members) {
            WanReplicationService service = wanService(member);
            if (service.hasWanReplicationScheme(WAN)) {
                for (var publisher : service.getWanReplicationPublishers(WAN).getPublishers()) {
                    if (publisher instanceof WanBatchPublisher batchPublisher) {
                        size += batchPublisher.getStats().getOutboundQueueSize();
                        assertThat(batchPublisher.getState()).isNotNull().isNotEqualTo(WanPublisherState.STOPPED);
                    }
                }
            }
        }
        return size;
    }

    private static int totalQueueSize(List<HecatonInstance> members) {
        int size = 0;
        for (HecatonInstance member : members) {
            for (var publisher : wanService(member).getWanReplicationPublishers(WAN).getPublishers()) {
                size += ((WanBatchPublisher) publisher).totalQueueSize();
            }
        }
        return size;
    }

    private static WanReplicationService wanService(HecatonInstance member) {
        return getNode(member).getNodeEngine().getWanReplicationService();
    }

    public static class RecordingConsumer implements WanConsumer {
        static final List<String> INITIALIZED = new CopyOnWriteArrayList<>();
        static final List<String> SHUT_DOWN = new CopyOnWriteArrayList<>();

        private String name;

        @Override
        public void init(String wanReplicationName, WanConsumerConfig config) {
            name = wanReplicationName;
            INITIALIZED.add(name);
        }

        @Override
        public void shutdown() {
            SHUT_DOWN.add(name);
        }
    }
}
