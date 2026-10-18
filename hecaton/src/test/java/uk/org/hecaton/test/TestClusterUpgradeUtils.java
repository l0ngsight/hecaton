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

package uk.org.hecaton.test;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.version.MemberVersion;
import uk.org.hecaton.version.Version;

import static uk.org.hecaton.instance.BuildInfoProvider.HECATON_INTERNAL_OVERRIDE_PREV_VERSION;
import static uk.org.hecaton.instance.BuildInfoProvider.HECATON_INTERNAL_OVERRIDE_VERSION;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.HecatonTestSupport.waitAllForSafeState;
import static org.junit.Assert.assertEquals;

/**
 * Utilities for cluster upgrade tests: start cluster members at designated version, assert cluster version etc.
 */
@SuppressWarnings("unused")
public final class TestClusterUpgradeUtils {

    private TestClusterUpgradeUtils() {
    }

    // return a new HecatonInstance at given version
    public static HecatonInstance newHecatonInstance(TestHecatonInstanceFactory factory, MemberVersion version,
                                                         Config config) {
        try {
            System.setProperty(HECATON_INTERNAL_OVERRIDE_VERSION, version.toString());
            return factory.newHecatonInstance(config);
        } finally {
            System.clearProperty(HECATON_INTERNAL_OVERRIDE_VERSION);
        }
    }

    // return a new HecatonInstance at given version
    public static HecatonInstance newHecatonInstance(TestHecatonInstanceFactory factory, MemberVersion version,
                                                         MemberVersion previousVersion, Config config) {
        try {
            System.setProperty(HECATON_INTERNAL_OVERRIDE_VERSION, version.toString());
            System.setProperty(HECATON_INTERNAL_OVERRIDE_PREV_VERSION, previousVersion.toString());
            return factory.newHecatonInstance(config);
        } finally {
            System.clearProperty(HECATON_INTERNAL_OVERRIDE_VERSION);
            System.clearProperty(HECATON_INTERNAL_OVERRIDE_PREV_VERSION);
        }
    }

    // shutdown and replace each member in membersToUpgrade with a new HecatonInstance at given version
    public static void upgradeClusterMembers(TestHecatonInstanceFactory factory, HecatonInstance[] membersToUpgrade,
                                             MemberVersion version, MemberVersion previousVersion, Config config) {
        upgradeClusterMembers(factory, membersToUpgrade, version, previousVersion, config, true);
    }

    public static void upgradeClusterMembers(TestHecatonInstanceFactory factory, final HecatonInstance[] membersToUpgrade,
                                             MemberVersion version, MemberVersion previousVersion,
                                             Config config, boolean assertClusterSize) {
        try {
            System.setProperty(HECATON_INTERNAL_OVERRIDE_VERSION, version.toString());
            System.setProperty(HECATON_INTERNAL_OVERRIDE_PREV_VERSION, previousVersion.toString());
            // upgrade one by one each member of the cluster to the next version
            for (int i = 0; i < membersToUpgrade.length; i++) {
                membersToUpgrade[i].shutdown();
                waitAllForSafeState(membersToUpgrade);
                // if new node's version is incompatible, then node startup will fail with IllegalStateException
                membersToUpgrade[i] = factory.newHecatonInstance(config);
                waitAllForSafeState(membersToUpgrade);
                if (assertClusterSize) {
                    // assert all members are in the cluster
                    assertTrueEventually(()
                            -> assertEquals(membersToUpgrade[0].toString(),
                            membersToUpgrade.length, membersToUpgrade[0].getCluster().getMembers().size()));
                }
            }
        } finally {
            System.clearProperty(HECATON_INTERNAL_OVERRIDE_VERSION);
            System.clearProperty(HECATON_INTERNAL_OVERRIDE_PREV_VERSION);
        }
    }

    // assert all members' clusterService reports the given version
    public static void assertClusterVersion(HecatonInstance[] instances, Version version) {
        for (HecatonInstance instance : instances) {
            assertEquals(version, instance.getCluster().getClusterVersion());
        }
    }

    // assert all nodes in the cluster have the given codebase version
    public static void assertNodesVersion(HecatonInstance[] instances, MemberVersion version) {
        for (HecatonInstance instance : instances) {
            assertEquals(version, getNode(instance).getVersion());
        }
    }
}
