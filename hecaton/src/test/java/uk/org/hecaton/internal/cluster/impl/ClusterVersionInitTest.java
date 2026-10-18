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

package uk.org.hecaton.internal.cluster.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.version.MemberVersion;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class ClusterVersionInitTest extends HecatonTestSupport {

    private HecatonInstance instance;
    private ClusterServiceImpl cluster;
    private MemberVersion codebaseVersion;

    @Test
    public void test_clusterVersion_isEventuallySet_whenSingleNodeMulticastJoinerCluster() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        setupInstance(config);
        assertEqualsEventually(() -> cluster.getClusterVersion(), codebaseVersion.asVersion());
    }

    @Test
    public void test_clusterVersion_isEventuallySet_whenNoJoinerConfiguredSingleNode() {
        Config config = new Config();
        config.setClusterName(randomName());
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        setupInstance(config);
        assertEqualsEventually(() -> cluster.getClusterVersion(), codebaseVersion.asVersion());
    }

    @Test
    public void test_clusterVersion_isEventuallySet_whenTcpJoinerConfiguredSingleNode() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        setupInstance(config);
        assertEqualsEventually(() -> cluster.getClusterVersion(), codebaseVersion.asVersion());
    }

    @Test
    public void test_clusterVersion_isEventuallySetOnJoiningMember_whenMulticastJoinerConfigured() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        setupInstance(config);
        HecatonInstance joiner = Hecaton.newHecatonInstance(config);
        ClusterServiceImpl joinerCluster = (ClusterServiceImpl) joiner.getCluster();

        assertEqualsEventually(joinerCluster::getClusterVersion, codebaseVersion.asVersion());

        joiner.shutdown();
    }

    @Test
    public void test_clusterVersion_isEventuallySetOnJoiningMember_whenTcpJoinerConfigured() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        setupInstance(config);
        HecatonInstance joiner = Hecaton.newHecatonInstance(config);
        ClusterServiceImpl joinerCluster = (ClusterServiceImpl) joiner.getCluster();

        assertEqualsEventually(joinerCluster::getClusterVersion, codebaseVersion.asVersion());

        joiner.shutdown();
    }

    private void setupInstance(Config config) {
        instance = Hecaton.newHecatonInstance(config);
        cluster = (ClusterServiceImpl) instance.getCluster();
        codebaseVersion = getNode(instance).getVersion();
    }

    @After
    public void tearDown() {
        instance.shutdown();
    }
}
