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

import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;

public class AuthenticationFailureOp extends AbstractClusterOperation {

    public AuthenticationFailureOp() {
    }

    @Override
    public void run() {
        final NodeEngine nodeEngine = getNodeEngine();
        final Node node = nodeEngine.getNode();
        JoinOperation.verifyCanShutdown(node, "Authentication failed on master node!");
        final ILogger logger = nodeEngine.getLogger("uk.org.hecaton.security");
        logger.severe("Node could not join cluster. Authentication failed on master node! Node is going to shutdown now!");
        node.shutdown(true);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.AUTH_FAILURE;
    }
}
