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
import uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.server.tcp.LinkedAddresses;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;

public class ClusterMismatchOp extends AbstractClusterOperation {

    public ClusterMismatchOp() {
    }

    @Override
    public void run() {
        NodeEngine nodeEngine = getNodeEngine();
        Connection connection = getConnection();

        String message = "Node could not join cluster at node: " + connection.getRemoteAddress()
                + " Cause: the target cluster has a different cluster-name";

        Node node = nodeEngine.getNode();
        LinkedAddresses linkedAddresses = node.getLocalAddressRegistry().linkedAddressesOf(getCallerUuid());

        connection.close(message, null);

        ILogger logger = nodeEngine.getLogger("uk.org.hecaton.cluster");
        logger.warning(message);
        if (linkedAddresses != null) {
            for (Address address : linkedAddresses.getAllAddresses()) {
                node.getJoiner().blacklist(address, true);
            }
        } else {
            node.getJoiner().blacklist(getCallerAddress(), true);
        }
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.CLUSTER_MISMATCH;
    }
}

