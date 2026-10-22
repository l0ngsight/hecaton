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

package uk.org.hecaton.client.impl.protocol.util;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.ClientMessageWriter;
import uk.org.hecaton.internal.networking.HandlerStatus;
import uk.org.hecaton.internal.networking.OutboundHandler;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

import static uk.org.hecaton.internal.networking.HandlerStatus.CLEAN;
import static uk.org.hecaton.internal.networking.HandlerStatus.DIRTY;
import static uk.org.hecaton.internal.nio.IOUtil.compactOrClear;

/**
 * A {@link OutboundHandler} for the new-client. It writes ClientMessages to the ByteBuffer.
 */
public class ClientMessageEncoder extends OutboundHandler<Supplier<ClientMessage>, ByteBuffer> {

    private ClientMessage message;
    private final ClientMessageWriter clientMessageWriter = new ClientMessageWriter();
    private final int bufferBytes;

    public ClientMessageEncoder() {
        this(-1);
    }

    /**
     * @param bufferBytes the size of the write buffer, or {@code -1} for the channel's send buffer
     *                    size; load protection (F23) sets it apart from the socket's
     */
    public ClientMessageEncoder(int bufferBytes) {
        this.bufferBytes = bufferBytes;
    }

    @Override
    public void handlerAdded() {
        if (bufferBytes > 0) {
            initDstBuffer(bufferBytes);
        } else {
            initDstBuffer();
        }
    }

    @Override
    public HandlerStatus onWrite() {
        compactOrClear(dst);
        try {
            for (; ; ) {
                if (message == null) {
                    message = src.get();

                    if (message == null) {
                        // everything is processed, so we are done
                        return CLEAN;
                    }
                }

                if (clientMessageWriter.writeTo(dst, message)) {
                    // message got written, lets see if another message can be written
                    message.written();
                    message = null;
                } else {
                    // the message didn't get written completely, so we are done.
                    return DIRTY;
                }
            }
        } finally {
            dst.flip();
        }
    }
}
