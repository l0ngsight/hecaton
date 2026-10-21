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

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.TcpIpConfig;
import uk.org.hecaton.cp.CPGroup;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.cp.CPSubsystemManagementService;
import uk.org.hecaton.cp.session.CPSession;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeState;
import uk.org.hecaton.internal.ascii.TextCommandService;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonArray;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.monitor.WanSyncState;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.server.Server;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.server.ServerConnectionManager;
import uk.org.hecaton.internal.util.StringUtil;
import uk.org.hecaton.logging.impl.LoggingServiceImpl;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.logging.Level;

import static uk.org.hecaton.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static uk.org.hecaton.instance.EndpointQualifier.CLIENT;
import static uk.org.hecaton.internal.ascii.rest.HttpCommandProcessor.ResponseType.SUCCESS;
import static uk.org.hecaton.internal.ascii.rest.HttpStatusCode.SC_500;
import static uk.org.hecaton.internal.ascii.rest.RestCallExecution.ObjectType.MAP;
import static uk.org.hecaton.internal.ascii.rest.RestCallExecution.ObjectType.QUEUE;
import static uk.org.hecaton.internal.util.ExceptionUtil.peel;
import static uk.org.hecaton.internal.util.StringUtil.equalsIgnoreCase;

@SuppressWarnings("checkstyle:methodcount")
public class HttpGetCommandProcessor extends HttpCommandProcessor<HttpGetCommand> {

    public static final String QUEUE_SIZE_COMMAND = "size";

    private static final String HEALTH_PATH_PARAM_NODE_STATE = "/node-state";
    private static final String HEALTH_PATH_PARAM_CLUSTER_STATE = "/cluster-state";
    private static final String HEALTH_PATH_PARAM_CLUSTER_SAFE = "/cluster-safe";
    private static final String HEALTH_PATH_PARAM_MIGRATION_QUEUE_SIZE = "/migration-queue-size";
    private static final String HEALTH_PATH_PARAM_CLUSTER_SIZE = "/cluster-size";

    public HttpGetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService, textCommandService.getNode().getLogger(HttpPostCommandProcessor.class));
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public void handle(HttpGetCommand command) {
        boolean sendResponse = true;
        try {
            String uri = command.getURI();
            if (uri.startsWith(URI_MAPS)) {
                handleMap(command, uri);
            } else if (uri.startsWith(URI_QUEUES)) {
                handleQueue(command, uri);
            } else if (uri.startsWith(URI_INSTANCE)) {
                handleInstance(command);
            } else if (uri.startsWith(URI_CLUSTER)) {
                handleCluster(command);
            } else if (uri.startsWith(URI_HEALTH_READY)) {
                handleHealthReady(command);
            } else if (uri.startsWith(URI_HEALTH_URL)) {
                handleHealthcheck(command, uri);
            } else if (uri.startsWith(URI_CLUSTER_VERSION_URL)) {
                handleGetClusterVersion(command);
            } else if (uri.startsWith(URI_LICENSE_INFO)) {
                handleLicense(command);
            } else if (uri.startsWith(URI_CP_GROUPS_URL)) {
                handleCPGroupRequest(command);
                sendResponse = false;
            } else if (uri.startsWith(URI_LOCAL_CP_MEMBER_URL)) {
                // this else if block must be above get-cp-members block
                handleGetLocalCPMember(command);
            } else if (uri.startsWith(URI_CP_MEMBERS_URL)) {
                handleGetCPMembers(command);
                sendResponse = false;
            } else if (uri.startsWith(URI_LOG_LEVEL)) {
                handleLogLevel(command);
            } else if (uri.startsWith(URI_TCP_IP_MEMBER_LIST)) {
                handleTcpIpMemberList(command);
            } else if (uri.startsWith(URI_WAN_SYNC_PROGRESS)) {
                handleWanSyncProgress(command);
            } else {
                command.send404();
            }
        } catch (IndexOutOfBoundsException e) {
            command.send400();
        } catch (Throwable e) {
            logger.warning("An error occurred while handling request " + command, e);
            prepareResponse(SC_500, command, exceptionResponse(e));
        }

        if (sendResponse) {
            textCommandService.sendResponse(command);
        }
    }

    private void handleHealthReady(HttpGetCommand command) {
        Node node = textCommandService.getNode();

        if (node.isRunning()
                && node.getNodeExtension().isReady()) {
            command.send200();
        } else {
            command.send503();
        }
    }

    private void handleHealthcheck(HttpGetCommand command, String uri) {
        Node node = textCommandService.getNode();
        NodeState nodeState = node.getState();

        ClusterServiceImpl clusterService = node.getClusterService();
        ClusterState clusterState = clusterService.getClusterState();
        int clusterSize = clusterService.getMembers().size();

        InternalPartitionService partitionService = node.getPartitionService();
        long migrationQueueSize = partitionService.getMigrationQueueSize();

        String healthParameter = uri.substring(URI_HEALTH_URL.length());
        if (healthParameter.equals(HEALTH_PATH_PARAM_NODE_STATE)) {
            if (NodeState.SHUT_DOWN == nodeState) {
                command.send503();
            } else {
                prepareResponse(command, Json.value(nodeState.toString()));
            }
        } else if (healthParameter.equals(HEALTH_PATH_PARAM_CLUSTER_STATE)) {
            prepareResponse(command, Json.value(clusterState.toString()));
        } else if (healthParameter.equals(HEALTH_PATH_PARAM_CLUSTER_SAFE)) {
            if (isClusterSafe()) {
                command.send200();
            } else {
                command.send503();
            }
        } else if (healthParameter.equals(HEALTH_PATH_PARAM_MIGRATION_QUEUE_SIZE)) {
            prepareResponse(command, Json.value(migrationQueueSize));
        } else if (healthParameter.equals(HEALTH_PATH_PARAM_CLUSTER_SIZE)) {
            prepareResponse(command, Json.value(clusterSize));
        } else if (healthParameter.isEmpty()) {
            JsonObject response = new JsonObject()
                    .add("nodeState", nodeState.toString())
                    .add("clusterState", clusterState.toString())
                    .add("clusterSafe", isClusterSafe())
                    .add("migrationQueueSize", migrationQueueSize)
                    .add("clusterSize", clusterSize);
            prepareResponse(command, response);
        } else {
            command.send400();
        }
    }

    private boolean isClusterSafe() {
        InternalPartitionService partitionService = textCommandService.getNode().getPartitionService();
        boolean memberStateSafe = partitionService.isMemberStateSafe();
        return memberStateSafe && !partitionService.hasOnGoingMigration();
    }

    private void handleGetClusterVersion(HttpGetCommand command) {
        Node node = textCommandService.getNode();
        ClusterService clusterService = node.getClusterService();
        JsonObject response = new JsonObject()
                .add("status", "success")
                .add("version", clusterService.getClusterVersion().toString());
        prepareResponse(command, response);
    }

    private void handleCPGroupRequest(HttpGetCommand command) {
        String uri = command.getURI();
        if (uri.contains(URI_CP_SESSIONS_SUFFIX)) {
            handleGetCPSessions(command);
        } else if (uri.endsWith(URI_CP_GROUPS_URL) || uri.endsWith(URI_CP_GROUPS_URL + "/")) {
            handleGetCPGroupIds(command);
        } else {
            handleGetCPGroupByName(command);
        }
    }

    private void handleGetCPGroupIds(final HttpGetCommand command) {
        CompletionStage<Collection<CPGroupId>> f = getCpSubsystemManagementService().getCPGroupIds();
        f.whenCompleteAsync((groupIds, t) -> {
            if (t == null) {
                JsonArray arr = new JsonArray();
                for (CPGroupId groupId : groupIds) {
                    arr.add(toJson(groupId));
                }
                prepareResponse(command, arr);
                textCommandService.sendResponse(command);
            } else {
                command.send500();
                textCommandService.sendResponse(command);
            }
        }, internalAsyncExecutor);
    }

    private void handleGetCPSessions(final HttpGetCommand command) {
        String uri = command.getURI();
        String prefix = URI_CP_GROUPS_URL + "/";
        int i = uri.indexOf(URI_CP_SESSIONS_SUFFIX);
        String groupName = uri.substring(prefix.length(), i).trim();
        getCpSubsystem().getCPSessionManagementService()
                .getAllSessions(groupName)
                .whenCompleteAsync((sessions, t) -> {
                    if (t == null) {
                        JsonArray sessionsArr = new JsonArray();
                        for (CPSession session : sessions) {
                            sessionsArr.add(toJson(session));
                        }
                        prepareResponse(command, sessionsArr);
                        textCommandService.sendResponse(command);
                    } else {
                        if (peel(t) instanceof IllegalArgumentException) {
                            command.send404();
                        } else {
                            command.send500();
                        }

                        textCommandService.sendResponse(command);
                    }
                }, internalAsyncExecutor);
    }

    private void handleGetCPGroupByName(final HttpGetCommand command) {
        String prefix = URI_CP_GROUPS_URL + "/";
        String groupName = command.getURI().substring(prefix.length()).trim();
        CompletionStage<CPGroup> f = getCpSubsystemManagementService().getCPGroup(groupName);
        f.whenCompleteAsync((group, t) -> {
            if (t == null) {
                if (group != null) {
                    JsonObject json = new JsonObject();
                    json.add("id", toJson(group.id()))
                            .add("status", group.status().name());

                    JsonArray membersArr = new JsonArray();
                    for (CPMember member : group.members()) {
                        membersArr.add(toJson(member));
                    }

                    json.add("members", membersArr);
                    prepareResponse(command, json);
                } else {
                    command.send404();
                }

                textCommandService.sendResponse(command);
            } else {
                command.send500();
                textCommandService.sendResponse(command);
            }
        }, internalAsyncExecutor);
    }

    private void handleGetCPMembers(final HttpGetCommand command) {
        CompletionStage<Collection<CPMember>> f = getCpSubsystemManagementService().getCPMembers();
        f.whenCompleteAsync((cpMembers, t) -> {
            if (t == null) {
                JsonArray arr = new JsonArray();
                for (CPMember cpMember : cpMembers) {
                    arr.add(toJson(cpMember));
                }
                prepareResponse(command, arr);
                textCommandService.sendResponse(command);
            } else {
                command.send500();
                textCommandService.sendResponse(command);
            }
        }, internalAsyncExecutor);
    }

    private void handleGetLocalCPMember(final HttpGetCommand command) {
        CPMember localCPMember = getCpSubsystem().getLocalCPMember();
        if (localCPMember != null) {
            prepareResponse(command, toJson(localCPMember));
        } else {
            command.send404();
        }
    }

    private CPSubsystemManagementService getCpSubsystemManagementService() {
        return getCpSubsystem().getCPSubsystemManagementService();
    }

    private CPSubsystem getCpSubsystem() {
        return textCommandService.getNode().getNodeEngine().getHecatonInstance().getCPSubsystem();
    }

    private JsonObject toJson(CPGroupId groupId) {
        return new JsonObject().add("name", groupId.getName()).add("id", groupId.getId());
    }

    private JsonObject toJson(CPMember cpMember) {
        Address address = cpMember.getAddress();
        return new JsonObject()
                .add("uuid", cpMember.getUuid().toString())
                .add("address", "[" + address.getHost() + "]:" + address.getPort());
    }

    private JsonObject toJson(CPSession cpSession) {
        Address address = cpSession.endpoint();
        return new JsonObject()
                .add("id", cpSession.id())
                .add("creationTime", cpSession.creationTime())
                .add("expirationTime", cpSession.expirationTime())
                .add("version", cpSession.version())
                .add("endpoint", "[" + address.getHost() + "]:" + address.getPort())
                .add("endpointType", cpSession.endpointType().name())
                .add("endpointName", cpSession.endpointName());
    }

    /**
     * Sets the HTTP response to a string containing basic cluster information:
     * <ul>
     * <li>Member list</li>
     * <li>Client connection count</li>
     * <li>Connection count</li>
     * </ul>
     *
     * @param command the HTTP request
     */
    private void handleCluster(HttpGetCommand command) {
        Node node = textCommandService.getNode();
        Server server = node.getServer();
        ClusterServiceImpl clusterService = node.getClusterService();
        JsonArray membersArray = new JsonArray();
        clusterService.getMembers()
                .stream()
                .map(m -> new JsonObject()
                        .add("address", m.getAddress().toString())
                        .add("liteMember", m.isLiteMember())
                        .add("localMember", m.localMember())
                        .add("uuid", m.getUuid().toString())
                        .add("memberVersion", m.getVersion().toString()))
                .forEach(membersArray::add);
        ServerConnectionManager cm = server.getConnectionManager(CLIENT);
        int clientCount = cm == null ? 0 : cm.connectionCount(ServerConnection::isClient);
        JsonObject response = new JsonObject()
                .add("members", membersArray)
                .add("connectionCount", clientCount)
                .add("allConnectionCount", server.connectionCount());
        prepareResponse(command, response);
    }

    /**
     * Sets the HTTP response to a string containing basic instance information in JSON format:
     * <ul>
     * <li>Instance name</li>
     * </ul>
     *
     * @param command the HTTP request
     */
    private void handleInstance(HttpGetCommand command) {
        prepareResponse(command, new JsonObject().add("name", textCommandService.getInstanceName()));
    }

    private void handleQueue(HttpGetCommand command, String uri) {
        command.getExecutionDetails().setObjectType(QUEUE);
        int indexEnd = uri.indexOf('/', URI_QUEUES.length());
        String queueName = uri.substring(URI_QUEUES.length(), indexEnd);
        command.getExecutionDetails().setObjectName(queueName);
        String secondStr = (uri.length() > (indexEnd + 1)) ? uri.substring(indexEnd + 1) : null;

        if (equalsIgnoreCase(QUEUE_SIZE_COMMAND, secondStr)) {
            int size = textCommandService.size(queueName);
            prepareResponse(command, Integer.toString(size));
        } else {
            int seconds = (secondStr == null) ? 0 : Integer.parseInt(secondStr);
            Object value = textCommandService.poll(queueName, seconds);
            prepareResponse(command, value);
        }
    }

    private void handleMap(HttpGetCommand command, String uri) {
        command.getExecutionDetails().setObjectType(MAP);
        uri = StringUtil.stripTrailingSlash(uri);
        int indexEnd = uri.indexOf('/', URI_MAPS.length());
        String mapName = uri.substring(URI_MAPS.length(), indexEnd);
        command.getExecutionDetails().setObjectName(mapName);
        String key = uri.substring(indexEnd + 1);
        Object value = textCommandService.get(mapName, key);
        prepareResponse(command, value);
    }

    @Override
    public void handleRejection(HttpGetCommand command) {
        handle(command);
    }

    /**
     * License info is implemented in the Enterprise GET command processor. The
     * OS version returns simple "404 Not Found".
     */
    protected void handleLicense(HttpGetCommand command) {
        command.send404();
    }

    private void handleLogLevel(HttpGetCommand command) {
        LoggingServiceImpl loggingService = (LoggingServiceImpl) getNode().getLoggingService();
        Level level = loggingService.getLevel();
        prepareResponse(command, new JsonObject().add("logLevel", level == null ? null : level.getName()));
    }

    private void handleTcpIpMemberList(HttpGetCommand command) {
        Config config = getNode().getConfig();
        TcpIpConfig tcpIpConfig = getActiveMemberNetworkConfig(config).getJoin().getTcpIpConfig();
        if (tcpIpConfig.isEnabled()) {
            List<String> members = tcpIpConfig.getMembers();
            JsonArray membersArray = new JsonArray();
            members.forEach(membersArray::add);
            prepareResponse(command, new JsonObject()
                    .add("status", "success")
                    .add("member-list", membersArray));
        } else {
            prepareResponse(HttpStatusCode.SC_400, command, new JsonObject().add("message",
                    "TCP-IP join mechanism is not enabled in the cluster."));
        }
    }

    protected void handleWanSyncProgress(HttpGetCommand command) {
        WanSyncState state = getNode().getNodeEngine().getWanReplicationService().getWanSyncState();
        JsonObject result = response(SUCCESS, "syncStatus", state.getStatus().name());
        if (state.getActiveWanConfigName() != null) {
            result.add("wanReplicationName", state.getActiveWanConfigName());
            result.add("publisherId", state.getActivePublisherName());
        }
        prepareResponse(command, result);
    }
}
