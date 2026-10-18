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

package uk.org.hecaton.internal.server.tcp;

import uk.org.hecaton.internal.networking.HandlerStatus;
import uk.org.hecaton.internal.networking.OutboundHandler;
import uk.org.hecaton.internal.nio.ConnectionType;
import uk.org.hecaton.internal.server.ServerConnection;


import java.nio.ByteBuffer;

import static uk.org.hecaton.internal.networking.HandlerStatus.CLEAN;
import static uk.org.hecaton.internal.networking.HandlerStatus.DIRTY;
import static uk.org.hecaton.internal.nio.IOUtil.compactOrClear;
import static uk.org.hecaton.internal.nio.Protocols.CLUSTER;
import static uk.org.hecaton.internal.nio.Protocols.PROTOCOL_LENGTH;
import static uk.org.hecaton.internal.util.StringUtil.stringToBytes;

/**
 * Writes the member protocol header bytes (HZC) to dst buffer and replaces itself by the next {@link OutboundHandler
 * OutboundHandlers}.
 */
public class MemberProtocolEncoder extends OutboundHandler<Void, ByteBuffer> {

    private final OutboundHandler[] outboundHandlers;

    /**
     * @param next the {@link OutboundHandler} to replace this one in the outbound pipeline
     *             upon match of protocol bytes
     */
    public MemberProtocolEncoder(OutboundHandler... next) {
        this.outboundHandlers = next;
    }

    @Override
    public void handlerAdded() {
        initDstBuffer(PROTOCOL_LENGTH, stringToBytes(CLUSTER));
    }

    @Override
    public HandlerStatus onWrite() {
        compactOrClear(dst);

        try {
            if (isProtocolBufferDrained()) {
                // replace!
                ServerConnection connection = (TcpServerConnection) channel.attributeMap().get(ServerConnection.class);
                connection.setConnectionType(ConnectionType.MEMBER);
                channel.outboundPipeline().replace(this, outboundHandlers);
                return CLEAN;
            }

            return DIRTY;
        } finally {
            dst.flip();
        }
    }

    /**
     * Checks if the protocol bytes have been drained.
     * <p>
     * The protocol buffer is in write mode, so if position is 0, the protocol
     * buffer has been drained.
     *
     * @return true if the protocol buffer has been drained.
     */
    private boolean isProtocolBufferDrained() {
        return dst.position() == 0;
    }
}
