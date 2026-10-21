/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.impl.protocol.task.management;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCGetCPMembersCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAsyncMessageTask;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.CPSubsystemManagementService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ManagementPermission;

import java.security.Permission;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;

public class GetCPMembersMessageTask extends AbstractAsyncMessageTask<Void, List<SimpleEntry<UUID, UUID>>> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("cp.getCPMembers");

    public GetCPMembersMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<List<SimpleEntry<UUID, UUID>>> processInternal() {
        CPSubsystemManagementService cpService =
                nodeEngine.getHecatonInstance().getCPSubsystem().getCPSubsystemManagementService();
        ClusterService clusterService = nodeEngine.getClusterService();
        return cpService.getCPMembers().toCompletableFuture()
                .thenApplyAsync(cpMembers -> {
                    List<SimpleEntry<UUID, UUID>> result = new ArrayList<>(cpMembers.size());
                    for (CPMember cpMember : cpMembers) {
                        Member member = clusterService.getMember(cpMember.getAddress());
                        UUID apUuid = member != null ? member.getUuid() : null;
                        result.add(new SimpleEntry<>(cpMember.getUuid(), apUuid));
                    }
                    return result;
                }, CALLER_RUNS);
    }

    @Override
    protected Void decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCGetCPMembersCodec.encodeResponse((List<Map.Entry<UUID, UUID>>) response);
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getCPMembers";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
