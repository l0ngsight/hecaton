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

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.NodeEngineImpl;

import static uk.org.hecaton.internal.util.ThreadUtil.createThreadName;

public class ShutdownNodeOp extends AbstractClusterOperation implements AllowedDuringPassiveState {

    @Override
    public void run() {
        final ClusterServiceImpl clusterService = getService();
        final ILogger logger = getLogger();

        final ClusterState clusterState = clusterService.getClusterState();

        if (clusterState == ClusterState.PASSIVE) {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            if (nodeEngine.isRunning()) {
                logger.info("Shutting down node in cluster passive state. Requested by: " + getCallerAddress());
                new Thread(() -> {
                    Node node = nodeEngine.getNode();
                    node.hecatonInstance.getLifecycleService().shutdown();
                }, createThreadName(nodeEngine.getHecatonInstance().getName(), ".clusterShutdown")).start();
            } else {
                logger.info("Node is already shutting down. NodeState: " + nodeEngine.getNode().getState());
            }
        } else {
            logger.severe("Can not shut down node because cluster is in " + clusterState + " state. Requested by: "
                    + getCallerAddress());
        }
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.SHUTDOWN_NODE;
    }
}
