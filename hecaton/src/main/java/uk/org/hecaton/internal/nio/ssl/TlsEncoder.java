/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.internal.nio.ssl;

import uk.org.hecaton.internal.networking.HandlerStatus;
import uk.org.hecaton.internal.networking.OutboundHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.nio.ByteBuffer;

import static uk.org.hecaton.internal.networking.ChannelOption.SO_SNDBUF;
import static uk.org.hecaton.internal.networking.HandlerStatus.BLOCKED;
import static uk.org.hecaton.internal.networking.HandlerStatus.CLEAN;
import static uk.org.hecaton.internal.networking.HandlerStatus.DIRTY;
import static uk.org.hecaton.internal.nio.IOUtil.compactOrClear;

/**
 * The last outbound handler of a TLS channel: encrypts what the previous handler produced (its
 * source) into the buffer that is written to the socket. It also sends the handshake records;
 * until the handshake is done, application data waits in the source.
 */
public class TlsEncoder extends OutboundHandler<ByteBuffer, ByteBuffer> {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private final SSLEngine engine;

    public TlsEncoder(SSLEngine engine) {
        this.engine = engine;
    }

    @Override
    public void handlerAdded() {
        initDstBuffer(Math.max(channel.options().getOption(SO_SNDBUF), engine.getSession().getPacketBufferSize()));
    }

    @Override
    public HandlerStatus onWrite() throws Exception {
        compactOrClear(dst);
        try {
            while (true) {
                SSLEngineResult result;
                switch (engine.getHandshakeStatus()) {
                    case NEED_TASK:
                        TlsHandshake.runDelegatedTasks(engine);
                        continue;
                    case NEED_UNWRAP:
                    case NEED_UNWRAP_AGAIN:
                        // waiting for the peer; the decoder wakes us up when there is something to send
                        channel.inboundPipeline().wakeup();
                        return BLOCKED;
                    case NEED_WRAP:
                        result = engine.wrap(EMPTY, dst);
                        break;
                    default:
                        if (src == null || !src.hasRemaining()) {
                            return CLEAN;
                        }
                        result = engine.wrap(src, dst);
                        break;
                }
                switch (result.getStatus()) {
                    case OK:
                        if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                            channel.inboundPipeline().wakeup();
                        }
                        break;
                    case BUFFER_OVERFLOW:
                        // the socket first needs to take what has been encrypted
                        return DIRTY;
                    case CLOSED:
                        throw new EOFException("The TLS connection is closed");
                    default:
                        throw new IllegalStateException("Unexpected " + result);
                }
            }
        } finally {
            dst.flip();
        }
    }

    @Override
    public void interceptError(Throwable error) throws Throwable {
        if (error instanceof SSLException) {
            throw new SSLException("TLS failure on " + channel + ": " + error.getMessage(), error);
        }
    }
}
