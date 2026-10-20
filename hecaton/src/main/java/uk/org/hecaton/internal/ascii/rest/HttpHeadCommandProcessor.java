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

package uk.org.hecaton.internal.ascii.rest;

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeState;
import uk.org.hecaton.internal.ascii.TextCommandService;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.partition.InternalPartitionService;

import java.util.LinkedHashMap;
import java.util.Map;

import static uk.org.hecaton.internal.ascii.rest.HttpStatusCode.SC_200;

public class HttpHeadCommandProcessor extends HttpCommandProcessor<HttpHeadCommand> {

    public HttpHeadCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService, textCommandService.getNode().getLogger(HttpPostCommandProcessor.class));
    }

    @Override
    public void handle(HttpHeadCommand command) {
        String uri = command.getURI();
        if (uri.startsWith(URI_MAPS)) {
            command.send200();
        } else if (uri.startsWith(URI_QUEUES)) {
            command.send200();
        } else if (uri.startsWith(URI_INSTANCE)) {
            command.send200();
        } else if (uri.startsWith(URI_CLUSTER)) {
            command.send200();
        } else if (uri.equals(URI_HEALTH_URL)) {
            handleHealthcheck(command);
        } else if (uri.startsWith(URI_CLUSTER_VERSION_URL)) {
            command.send200();
        } else if (uri.startsWith(URI_LOG_LEVEL)) {
            command.send200();
        } else {
            command.send404();
        }
        textCommandService.sendResponse(command);
    }

    private void handleHealthcheck(HttpHeadCommand command) {
        Node node = textCommandService.getNode();
        NodeState nodeState = node.getState();

        ClusterServiceImpl clusterService = node.getClusterService();
        ClusterState clusterState = clusterService.getClusterState();
        int clusterSize = clusterService.getMembers().size();

        InternalPartitionService partitionService = node.getPartitionService();
        long migrationQueueSize = partitionService.getMigrationQueueSize();

        Map<String, Object> headervals = new LinkedHashMap<>();
        headervals.put("NodeState", nodeState);
        headervals.put("ClusterState", clusterState);
        headervals.put("MigrationQueueSize", migrationQueueSize);
        headervals.put("ClusterSize", clusterSize);

        command.setResponse(SC_200, headervals);
    }

    @Override
    public void handleRejection(HttpHeadCommand command) {
        handle(command);
    }

}
