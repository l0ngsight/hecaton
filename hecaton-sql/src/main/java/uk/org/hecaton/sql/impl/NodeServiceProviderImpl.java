/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.sql.impl;

import uk.org.hecaton.client.Client;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.cluster.memberselector.MemberSelectors;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Default node service provider implementation.
 */
public class NodeServiceProviderImpl implements NodeServiceProvider {

    private final NodeEngineImpl nodeEngine;

    public NodeServiceProviderImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public UUID getLocalMemberId() {
        return nodeEngine.getClusterService().getLocalMember().getUuid();
    }

    @Override
    public Collection<UUID> getDataMemberIds() {
        Set<UUID> res = new HashSet<>();

        for (Member member : nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR)) {
            res.add(member.getUuid());
        }

        return res;
    }

    @Override
    public Set<UUID> getClientIds() {
        Set<UUID> res = new HashSet<>();

        for (Client client : nodeEngine.getHecatonInstance().getClientService().getConnectedClients()) {
            res.add(client.getUuid());
        }

        return res;
    }

    @Override
    public Connection getConnection(UUID memberId) {
        MemberImpl member = nodeEngine.getClusterService().getMember(memberId);

        if (member == null) {
            return null;
        }

        return nodeEngine.getNode()
                .getServer()
                .getConnectionManager(EndpointQualifier.MEMBER)
                .getOrConnect(member.getAddress());
    }

    @Override
    public MapContainer getMap(String name) {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);

        return mapService.getMapServiceContext().getMapContainers().get(name);
    }

    @Override
    public ILogger getLogger(Class<?> clazz) {
        return nodeEngine.getLogger(clazz);
    }
}
