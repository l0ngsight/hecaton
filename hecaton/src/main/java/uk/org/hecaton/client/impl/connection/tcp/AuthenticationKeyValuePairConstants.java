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

package uk.org.hecaton.client.impl.connection.tcp;

import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.client.UnsupportedClusterVersionException;
import uk.org.hecaton.client.UnsupportedRoutingModeException;
import uk.org.hecaton.client.config.ClusterRoutingConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.internal.cluster.Versions;
import uk.org.hecaton.version.Version;

import java.util.Map;

import static uk.org.hecaton.client.config.RoutingStrategy.PARTITION_GROUPS;

/**
 * Client uses the constants defined in this class to look up fields
 * in keyValuePairs which is sent from the server-side.
 *
 * @see KeyValuePairGenerator
 */
public final class AuthenticationKeyValuePairConstants {

    public static final String CLUSTER_VERSION = "clusterVersion";
    public static final String ROUTING_MODE_NOT_SUPPORTED_MESSAGE;
    public static final String MEMBER_GROUPS_INFO = "memberGroups";
    public static final String CP_LEADERS_INFO = "cp.leaders";
    private static final Version MULTI_MEMBER_ROUTING_MINIMUM_SUPPORTED_CLUSTER_VERSION = Versions.V5_5;

    static {
        ROUTING_MODE_NOT_SUPPORTED_MESSAGE = String.format(
                "%s routing mode %s cannot be supported because the server has not sent "
                        + "the required information (cluster version %s or later needed). %s",
                RoutingMode.MULTI_MEMBER, PARTITION_GROUPS, MULTI_MEMBER_ROUTING_MINIMUM_SUPPORTED_CLUSTER_VERSION,
                HecatonFeature.MULTI_MEMBER_ROUTING.notAvailableMessage());
    }

    private AuthenticationKeyValuePairConstants() { }

    public static boolean checkRequiredFieldsForMultiMemberRoutingExist(ClusterRoutingConfig clusterRoutingConfig,
                                                                        Map<String, String> keyValuePairs) {
        if (clusterRoutingConfig.getRoutingMode() != RoutingMode.MULTI_MEMBER) {
            return false;
        }

        if (clusterRoutingConfig.getRoutingStrategy() != PARTITION_GROUPS) {
            return false;
        }

        if (!keyValuePairs.containsKey(MEMBER_GROUPS_INFO)) {
            throw new UnsupportedRoutingModeException(ROUTING_MODE_NOT_SUPPORTED_MESSAGE);
        }

        return true;
    }

    public static void checkMinimumClusterVersionForMultiMemberRouting(Map<String, String> keyValuePairs) {
        if (!keyValuePairs.containsKey(CLUSTER_VERSION)
                || Version.of(keyValuePairs.get(CLUSTER_VERSION)).isUnknown()
                || Version.of(keyValuePairs.get(CLUSTER_VERSION)).isLessThan(
                MULTI_MEMBER_ROUTING_MINIMUM_SUPPORTED_CLUSTER_VERSION)) {
            throw new UnsupportedClusterVersionException(ROUTING_MODE_NOT_SUPPORTED_MESSAGE);
        }
    }
}
