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

package uk.org.hecaton.spi.impl.operationservice.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.server.ServerConnectionManager;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import static uk.org.hecaton.instance.EndpointQualifier.MEMBER;
import static uk.org.hecaton.internal.nio.Packet.FLAG_URGENT;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * Responsible for sending Operations to another member.
 */
public class OutboundOperationHandler {
    private final Address thisAddress;
    private final InternalSerializationService serializationService;
    private final Node node;

    public OutboundOperationHandler(Node node, InternalSerializationService serializationService) {
        this.node = node;
        this.thisAddress = node.getThisAddress();
        this.serializationService = serializationService;
    }

    public boolean send(Operation op, Address target) {
        return send(op, target, node.getServer().getConnectionManager(MEMBER));
    }

    public boolean send(Operation op, Address target, ServerConnectionManager cm) {
        if (cm == null) {
            cm = node.getServer().getConnectionManager(MEMBER);
        }
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            return false;
        }

        int streamId = op.getPartitionId();
        return cm.transmit(toPacket(op), target, streamId);
    }

    public boolean send(Operation op, ServerConnection connection) {
        Packet packet = toPacket(op);
        return connection.write(packet);
    }

    private Packet toPacket(Operation op) {
        byte[] bytes = serializationService.toBytes(op);
        int partitionId = op.getPartitionId();
        Packet packet = new Packet(bytes, partitionId).setPacketType(Packet.Type.OPERATION);

        if (op.isUrgent()) {
            packet.raiseFlags(FLAG_URGENT);
        }
        return packet;
    }
}
