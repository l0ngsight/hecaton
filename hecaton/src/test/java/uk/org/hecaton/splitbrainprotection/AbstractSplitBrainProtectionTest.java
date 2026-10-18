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

package uk.org.hecaton.splitbrainprotection;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cardinality.CardinalityEstimator;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CardinalityEstimatorConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.config.ExecutorConfig;
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.config.PNCounterConfig;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.config.ScheduledExecutorConfig;
import uk.org.hecaton.config.SetConfig;
import uk.org.hecaton.config.SplitBrainProtectionConfig;
import uk.org.hecaton.config.vector.Metric;
import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.config.vector.VectorIndexConfig;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.core.IFunction;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.durableexecutor.DurableExecutorService;
import uk.org.hecaton.function.ConsumerEx;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.scheduledexecutor.IScheduledExecutorService;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.TestLoggingUtils;

import java.util.function.Consumer;

import static uk.org.hecaton.function.ConsumerEx.noop;
import static uk.org.hecaton.splitbrainprotection.PartitionedCluster.SPLIT_BRAIN_PROTECTION_ID;
import static uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn.READ;
import static uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static java.util.Arrays.asList;

/**
 * Base class for all split brain protection tests.
 * <p>
 * It defines split brain protection and data-structures that use it.
 * Then it initialises and splits the cluster into two parts:
 * <ul>
 * <li>3 nodes -> this sub-cluster matches the split brain protection requirements</li>
 * <li>2 nodes -> this sub-cluster DOES NOT match the split brain protection requirements</li>
 * </ul>
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractSplitBrainProtectionTest extends HecatonTestSupport {

    protected static final String CACHE_NAME = "splitBrainProtection-cache-" + randomString();
    protected static final String ESTIMATOR_NAME = "splitBrainProtection-estimator-" + randomString();
    protected static final String DURABLE_EXEC_NAME = "splitBrainProtection-durable-exec-" + randomString();
    protected static final String EXEC_NAME = "splitBrainProtection-exec-" + randomString();
    protected static final String LIST_NAME = "splitBrainProtection-list-" + randomString();
    protected static final String MAP_NAME = "splitBrainProtection-map-" + randomString();
    protected static final String MULTI_MAP_NAME = "splitBrainProtection-multimap-" + randomString();
    protected static final String QUEUE_NAME = "splitBrainProtection-queue-" + randomString();
    protected static final String REPLICATED_MAP_NAME = "splitBrainProtection-replicated-map-" + randomString();
    protected static final String RINGBUFFER_NAME = "splitBrainProtection-ringbuffer-" + randomString();
    protected static final String SCHEDULED_EXEC_NAME = "splitBrainProtection-scheduled-exec-" + randomString();
    protected static final String SET_NAME = "splitBrainProtection-set-" + randomString();
    protected static final String PN_COUNTER_NAME = "splitBrainProtection-pn-counter-" + randomString();
    protected static final String VECTOR_COLLECTION_NAME = "splitBrainProtection-vector-collection-" + randomString();
    private static final ILogger LOGGER;

    protected static PartitionedCluster cluster;
    protected static TestHecatonInstanceFactory factory;

    // Initializes logging. Normally doing this once should be fine. But for some reason
    // Java can't get the system property properly. So I initialize here second time aside
    // from AbstractHecatonClassRunner#initialize()
    static {
        TestLoggingUtils.initializeLogging();
        LOGGER = Logger.getLogger(AbstractSplitBrainProtectionTest.class);
    }

    protected static void initTestEnvironment(Config config,
                                              TestHecatonInstanceFactory factory) {
        initTestEnvironment(config, factory, noop());
    }

    protected static void initTestEnvironment(Config config,
                                              TestHecatonInstanceFactory factory,
                                              ConsumerEx<SplitBrainProtectionOn[]> additionalInitData) {
        if (AbstractSplitBrainProtectionTest.factory != null) {
            throw new IllegalStateException("Already initialised!");
        }
        AbstractSplitBrainProtectionTest.factory = factory;
        initCluster(PartitionedCluster.createClusterConfig(config), factory, additionalInitData, READ, WRITE, READ_WRITE);
    }

    protected static void shutdownTestEnvironment() {
        factory.terminateAll();
        factory = null;
        cluster = null;
    }

    protected static CacheSimpleConfig newCacheConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                      String splitBrainProtectionName) {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setName(CACHE_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static CardinalityEstimatorConfig newEstimatorConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                   String splitBrainProtectionName) {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(ESTIMATOR_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static DurableExecutorConfig newDurableExecConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                String splitBrainProtectionName, String postfix) {
        DurableExecutorConfig config = new DurableExecutorConfig(DURABLE_EXEC_NAME + splitBrainProtectionOn.name() + postfix);
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static ExecutorConfig newExecConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                  String splitBrainProtectionName, String postfix) {
        ExecutorConfig config = new ExecutorConfig(EXEC_NAME + splitBrainProtectionOn.name() + postfix);
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static ListConfig newListConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        ListConfig config = new ListConfig(LIST_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static MapConfig newMapConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        MapConfig config = new MapConfig(MAP_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static MultiMapConfig newMultiMapConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                      String splitBrainProtectionName) {
        MultiMapConfig config = new MultiMapConfig(MULTI_MAP_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static QueueConfig newQueueConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        QueueConfig config = new QueueConfig(QUEUE_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        config.setBackupCount(4);
        return config;
    }

    protected static ReplicatedMapConfig newReplicatedMapConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                String splitBrainProtectionName) {
        ReplicatedMapConfig config = new ReplicatedMapConfig(REPLICATED_MAP_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static RingbufferConfig newRingbufferConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                          String splitBrainProtectionName) {
        RingbufferConfig config = new RingbufferConfig(RINGBUFFER_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        config.setBackupCount(4);
        return config;
    }

    protected static ScheduledExecutorConfig newScheduledExecConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                    String splitBrainProtectionName, String postfix) {
        ScheduledExecutorConfig config =
                new ScheduledExecutorConfig(SCHEDULED_EXEC_NAME + splitBrainProtectionOn.name() + postfix);
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static SetConfig newSetConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        SetConfig config = new SetConfig(SET_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static PNCounterConfig newPNCounterConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                        String splitBrainProtectionName) {
        PNCounterConfig config = new PNCounterConfig(PN_COUNTER_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static VectorCollectionConfig newVectorCollectionConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                      String splitBrainProtectionName) {
        var config = new VectorCollectionConfig(VECTOR_COLLECTION_NAME + splitBrainProtectionOn.name());
        config.addVectorIndexConfig(new VectorIndexConfig("test", Metric.EUCLIDEAN, 2));
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static SplitBrainProtectionConfig newSplitBrainProtectionConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                              String splitBrainProtectionName) {
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig();
        splitBrainProtectionConfig.setName(splitBrainProtectionName);
        splitBrainProtectionConfig.setProtectOn(splitBrainProtectionOn);
        splitBrainProtectionConfig.setEnabled(true);
        splitBrainProtectionConfig.setMinimumClusterSize(3);
        return splitBrainProtectionConfig;
    }

    protected static void initCluster(Config config, TestHecatonInstanceFactory factory, Consumer<SplitBrainProtectionOn[]> additionalInitData, SplitBrainProtectionOn... types) {
        cluster = new PartitionedCluster(factory);

        String[] splitBrainProtectionNames = new String[types.length];
        int i = 0;
        for (SplitBrainProtectionOn splitBrainProtectionOn : types) {
            String splitBrainProtectionName = SPLIT_BRAIN_PROTECTION_ID + splitBrainProtectionOn.name();
            config.addSplitBrainProtectionConfig(newSplitBrainProtectionConfig(splitBrainProtectionOn, splitBrainProtectionName));
            splitBrainProtectionNames[i++] = splitBrainProtectionName;

            config.addCacheConfig(newCacheConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addCardinalityEstimatorConfig(newEstimatorConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addListConfig(newListConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addMapConfig(newMapConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addMultiMapConfig(newMultiMapConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addQueueConfig(newQueueConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addReplicatedMapConfig(newReplicatedMapConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addRingBufferConfig(newRingbufferConfig(splitBrainProtectionOn, splitBrainProtectionName));
            for (String postfix : asList("", "shutdown", "shutdownNow")) {
                config.addDurableExecutorConfig(newDurableExecConfig(splitBrainProtectionOn, splitBrainProtectionName, postfix));
                config.addExecutorConfig(newExecConfig(splitBrainProtectionOn, splitBrainProtectionName, postfix));
                config.addScheduledExecutorConfig(
                        newScheduledExecConfig(splitBrainProtectionOn, splitBrainProtectionName, postfix));
            }
            config.addSetConfig(newSetConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addPNCounterConfig(newPNCounterConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addVectorCollectionConfig(newVectorCollectionConfig(splitBrainProtectionOn, splitBrainProtectionName));
        }
        cluster.createFiveMemberCluster(config, splitBrainProtectionNames);
        for (SplitBrainProtectionOn splitBrainProtectionOn : types) {
            LOGGER.info("Queue size before data initialization for "
                    + splitBrainProtectionOn
                    + " is: "
                    + cluster.instance[0].getQueue(QUEUE_NAME + splitBrainProtectionOn.name()).size());
        }
        initData(types);
        additionalInitData.accept(types);
        for (SplitBrainProtectionOn splitBrainProtectionOn : types) {
            LOGGER.info("Queue size after data initialization for "
                    + splitBrainProtectionOn
                    + " is: "
                    + cluster.instance[0].getQueue(QUEUE_NAME + splitBrainProtectionOn.name()).size());
        }
        cluster.splitFiveMembersThreeAndTwo(splitBrainProtectionNames);
        for (SplitBrainProtectionOn splitBrainProtectionOn : types) {
            for (int k = 0; k < 3; k++) {
                LOGGER.info("Queue size after data initialization for instance "
                        + k
                        + " for type "
                        + splitBrainProtectionOn
                        + " is: "
                        + cluster.instance[k].getQueue(QUEUE_NAME + splitBrainProtectionOn.name()).size());
            }
        }
    }

    private static void initData(SplitBrainProtectionOn[] types) {
        for (SplitBrainProtectionOn splitBrainProtectionOn : types) {
            for (int element = 0; element < 10000; element++) {
                cluster.instance[0].getQueue(QUEUE_NAME + splitBrainProtectionOn.name()).offer(element);
            }
            for (int id = 0; id < 10000; id++) {
                cluster.instance[0].getRingbuffer(RINGBUFFER_NAME + splitBrainProtectionOn.name()).add(String.valueOf(id));
            }
        }
    }

    protected ICache<Integer, String> cache(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getCacheManager().getCache(CACHE_NAME + splitBrainProtectionOn.name());
    }

    protected CardinalityEstimator estimator(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getCardinalityEstimator(ESTIMATOR_NAME + splitBrainProtectionOn.name());
    }

    protected DurableExecutorService durableExec(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return durableExec(index, splitBrainProtectionOn, "");
    }

    protected DurableExecutorService durableExec(int index, SplitBrainProtectionOn splitBrainProtectionOn, String postfix) {
        return cluster.instance[index].getDurableExecutorService(DURABLE_EXEC_NAME + splitBrainProtectionOn.name() + postfix);
    }

    protected IExecutorService exec(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return exec(index, splitBrainProtectionOn, "");
    }

    protected IExecutorService exec(int index, SplitBrainProtectionOn splitBrainProtectionOn, String postfix) {
        return cluster.instance[index].getExecutorService(EXEC_NAME + splitBrainProtectionOn.name() + postfix);
    }

    protected IList list(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getList(LIST_NAME + splitBrainProtectionOn.name());
    }

    protected IMap map(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getMap(MAP_NAME + splitBrainProtectionOn.name());
    }

    protected MultiMap multimap(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getMultiMap(MULTI_MAP_NAME + splitBrainProtectionOn.name());
    }

    protected IQueue queue(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getQueue(QUEUE_NAME + splitBrainProtectionOn.name());
    }

    protected ReplicatedMap replmap(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getReplicatedMap(REPLICATED_MAP_NAME + splitBrainProtectionOn.name());
    }

    protected Ringbuffer ring(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getRingbuffer(RINGBUFFER_NAME + splitBrainProtectionOn.name());
    }

    protected IScheduledExecutorService scheduledExec(int index, SplitBrainProtectionOn splitBrainProtectionOn, String postfix) {
        return cluster.instance[index].getScheduledExecutorService(SCHEDULED_EXEC_NAME + splitBrainProtectionOn.name() + postfix);
    }

    protected ISet set(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getSet(SET_NAME + splitBrainProtectionOn.name());
    }

    protected PNCounter pnCounter(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getPNCounter(PN_COUNTER_NAME + splitBrainProtectionOn.name());
    }

    protected static IFunction function() {
        return (IFunction<Object, Object>) input -> input;
    }

}
