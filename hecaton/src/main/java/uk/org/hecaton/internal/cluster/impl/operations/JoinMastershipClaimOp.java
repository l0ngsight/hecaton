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

package uk.org.hecaton.internal.cluster.impl.operations;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.Joiner;
import uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.cluster.impl.TcpIpJoiner;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;

public class JoinMastershipClaimOp extends AbstractJoinOperation {

    private transient boolean approvedAsMaster;

    @Override
    public void run() {
        final NodeEngine nodeEngine = getNodeEngine();
        Node node = nodeEngine.getNode();
        Joiner joiner = node.getJoiner();
        ClusterServiceImpl clusterService = node.getClusterService();
        final ILogger logger = node.getLogger(getClass().getName());
        if (joiner instanceof TcpIpJoiner tcpIpJoiner) {
            final Address endpoint = getCallerAddress();
            final Address masterAddress = clusterService.getMasterAddress();
            approvedAsMaster = !tcpIpJoiner.isClaimingMastership() && !clusterService.isMaster()
                    && (masterAddress == null || masterAddress.equals(endpoint));
        } else {
            approvedAsMaster = false;
            logger.warning("This node requires MulticastJoin strategy!");
        }
        if (logger.isFineEnabled()) {
            logger.fine("Sending '%s' for master claim of node: %s", approvedAsMaster, getCallerAddress());
        }
    }

    @Override
    public Object getResponse() {
        return approvedAsMaster;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MASTER_CLAIM;
    }
}
