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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.instance.impl.HecatonInstanceProxy;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeState;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.SplitBrainTestSupport;
import org.junit.Before;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static uk.org.hecaton.internal.util.Preconditions.checkPositive;

/**
 * A support class for high-level split-brain tests. It will form a
 * cluster, create a split-brain situation, and then heal the cluster
 * again.
 * <p>
 * Tests are supposed to extend this class and use
 * {@link JetSplitBrainTestSupport#testSplitBrain(int, int, Consumer, BiConsumer, Consumer)} by providing
 * hooks to be notified about state transitions. All hooks are optional.
 * <p>
 * See {@link SplitBrainTest} for examples.
 */
public abstract class JetSplitBrainTestSupport extends JetTestSupport {

    static final int PARALLELISM = 4;

    private static final ILogger LOGGER = Logger.getLogger(JetSplitBrainTestSupport.class);

    /**
     * If new nodes have been created during split brain via
     * {@link SplitBrainTestSupport#createHecatonInstanceInBrain(int)}, then their joiners
     * are initialized with the other brain's addresses being blacklisted.
     */
    private boolean unblacklistHint;
    protected Config config;

    @Before
    public final void setUpInternals() {
        onBeforeSetup();
    }

    /**
     * Override this method to execute initialization that may be required
     * before instantiating the cluster. This is the
     * first method executed by {@code @Before SplitBrainTestSupport.setupInternals}.
     */
    protected void onBeforeSetup() {
    }

    protected Config createConfig() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(PARALLELISM);
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        onConfigCreated(config);
        return config;
    }

    /**
     * Override this for custom Jet configuration
     */
    protected void onConfigCreated(Config config) {
    }

    protected final void testSplitBrain(int firstSubClusterSize, int secondSubClusterSize,
                                        Consumer<HecatonInstance[]> beforeSplit,
                                        BiConsumer<HecatonInstance[], HecatonInstance[]> onSplit,
                                        Consumer<HecatonInstance[]> afterMerge) {
        testSplitBrain(firstSubClusterSize, secondSubClusterSize, beforeSplit, onSplit, afterMerge, 1);
    }

    protected final void testSplitBrain(int firstSubClusterSize, int secondSubClusterSize,
                                        Consumer<HecatonInstance[]> beforeSplit,
                                        BiConsumer<HecatonInstance[], HecatonInstance[]> onSplit,
                                        Consumer<HecatonInstance[]> afterMerge,
                                        int numberOfRepeats) {
        checkPositive(firstSubClusterSize, "invalid first sub cluster size: " + firstSubClusterSize);
        checkPositive(secondSubClusterSize, "invalid second sub cluster size: " + secondSubClusterSize);

        config = createConfig();
        Config liteMemberConfig = createConfig().setLiteMember(true);
        int clusterSize = firstSubClusterSize + secondSubClusterSize;
        HecatonInstance[] instances = startInitialCluster(config, liteMemberConfig, clusterSize);

        if (beforeSplit != null) {
            beforeSplit.accept(instances);
        }

        for (int splitBrainNumber = 0; splitBrainNumber < numberOfRepeats; ++splitBrainNumber) {

            LOGGER.info("Going to create split-brain... #" + splitBrainNumber);
            createSplitBrain(instances, firstSubClusterSize, secondSubClusterSize);
            Brains brains = getBrains(instances, firstSubClusterSize, secondSubClusterSize);
            LOGGER.info("Split-brain created");

            if (onSplit != null) {
                onSplit.accept(brains.firstSubCluster, brains.secondSubCluster);
            }

            LOGGER.info("Going to heal split-brain... #" + splitBrainNumber);
            healSplitBrain(instances, firstSubClusterSize);
            LOGGER.info("Split-brain healed");

            if (afterMerge != null) {
                afterMerge.accept(instances);
            }
        }
    }

    protected HecatonInstance[] startInitialCluster(Config config, @Nullable Config liteConfig, int clusterSize) {
        HecatonInstance[] instances = new HecatonInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = createHecatonInstance(config);
        }
        return instances;
    }

    private void createSplitBrain(HecatonInstance[] instances, int firstSubClusterSize, int secondSubClusterSize) {
        applyOnBrains(instances, firstSubClusterSize, SplitBrainTestSupport::blockCommunicationBetween);
        applyOnBrains(instances, firstSubClusterSize, HecatonTestSupport::closeConnectionBetween);
        assertSplitBrainCreated(instances, firstSubClusterSize, secondSubClusterSize);
    }

    private void assertSplitBrainCreated(HecatonInstance[] instances, int firstSubClusterSize, int secondSubClusterSize) {
        for (int isolatedIndex = 0; isolatedIndex < firstSubClusterSize; isolatedIndex++) {
            HecatonInstance isolatedInstance = instances[isolatedIndex];
            assertClusterSizeEventually(firstSubClusterSize, isolatedInstance);
        }
        for (int i = firstSubClusterSize; i < instances.length; i++) {
            HecatonInstance currentInstance = instances[i];
            assertClusterSizeEventually(secondSubClusterSize, currentInstance);
        }
    }

    private void healSplitBrain(HecatonInstance[] instances, int firstSubClusterSize) {
        applyOnBrains(instances, firstSubClusterSize, SplitBrainTestSupport::unblockCommunicationBetween);
        if (unblacklistHint) {
            applyOnBrains(instances, firstSubClusterSize, JetSplitBrainTestSupport::unblacklistJoinerBetween);
        }
        for (HecatonInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }
        waitAllForSafeState(instances);
    }

    private Brains getBrains(HecatonInstance[] instances, int firstSubClusterSize, int secondSubClusterSize) {
        HecatonInstance[] firstSubCluster = new HecatonInstance[firstSubClusterSize];
        HecatonInstance[] secondSubCluster = new HecatonInstance[secondSubClusterSize];
        for (int i = 0; i < instances.length; i++) {
            if (i < firstSubClusterSize) {
                firstSubCluster[i] = instances[i];
            } else {
                secondSubCluster[i - firstSubClusterSize] = instances[i];
            }
        }
        return new Brains(firstSubCluster, secondSubCluster);
    }

    private void applyOnBrains(HecatonInstance[] instances, int firstSubClusterSize,
                               BiConsumer<HecatonInstance, HecatonInstance> action) {
        for (int i = 0; i < firstSubClusterSize; i++) {
            HecatonInstance isolatedInstance = instances[i];
            // do not take into account instances which have been shutdown
            if (!isInstanceActive(isolatedInstance)) {
                continue;
            }
            for (int j = firstSubClusterSize; j < instances.length; j++) {
                HecatonInstance currentInstance = instances[j];
                if (!isInstanceActive(currentInstance)) {
                    continue;
                }
                action.accept(isolatedInstance, currentInstance);
            }
        }
    }

    private static boolean isInstanceActive(HecatonInstance instance) {
        if (instance instanceof HecatonInstanceProxy proxy) {
            try {
                proxy.getOriginal();
                return true;
            } catch (HecatonInstanceNotActiveException exception) {
                return false;
            }
        } else if (instance instanceof HecatonInstanceImpl) {
            return Accessors.getNode(instance).getState() == NodeState.ACTIVE;
        } else {
            throw new AssertionError("Unsupported HecatonInstance type");
        }
    }

    private static void unblacklistJoinerBetween(HecatonInstance h1, HecatonInstance h2) {
        Node h1Node = Accessors.getNode(h1);
        Node h2Node = Accessors.getNode(h2);
        h1Node.getJoiner().unblacklist(h2Node.getThisAddress());
        h2Node.getJoiner().unblacklist(h1Node.getThisAddress());
    }

    private static final class Brains {
        private final HecatonInstance[] firstSubCluster;
        private final HecatonInstance[] secondSubCluster;

        private Brains(HecatonInstance[] firstSubCluster, HecatonInstance[] secondSubCluster) {
            this.firstSubCluster = firstSubCluster;
            this.secondSubCluster = secondSubCluster;
        }
    }
}

