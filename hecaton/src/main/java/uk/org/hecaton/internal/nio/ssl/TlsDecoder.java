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
import uk.org.hecaton.internal.networking.InboundHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.io.EOFException;
import java.nio.ByteBuffer;

import static uk.org.hecaton.internal.networking.ChannelOption.SO_RCVBUF;
import static uk.org.hecaton.internal.networking.HandlerStatus.CLEAN;
import static uk.org.hecaton.internal.networking.HandlerStatus.DIRTY;
import static uk.org.hecaton.internal.nio.IOUtil.compactOrClear;

/**
 * The first inbound handler of a TLS channel: decrypts the bytes read from the socket (its
 * source) and passes the plain bytes on to the next handler's source, as much as it takes at a
 * time (the protocol decoders first read just the protocol header). Handshake records are
 * processed here too; when the handshake needs to send something, the outbound pipeline is woken
 * up so that the {@link TlsEncoder} sends it.
 */
public class TlsDecoder extends InboundHandler<ByteBuffer, ByteBuffer> {

    private final SSLEngine engine;
    /** Decrypted bytes not yet passed on; in reading mode. */
    private ByteBuffer plain;

    public TlsDecoder(SSLEngine engine) {
        this.engine = engine;
    }

    @Override
    public void handlerAdded() {
        initSrcBuffer(Math.max(channel.options().getOption(SO_RCVBUF), engine.getSession().getPacketBufferSize()));
        plain = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
        plain.flip();
    }

    @Override
    public HandlerStatus onRead() throws Exception {
        src.flip();
        try {
            while (true) {
                if (!passOn()) {
                    // the next handlers first need to consume what has been passed on
                    return DIRTY;
                }
                switch (engine.getHandshakeStatus()) {
                    case NEED_TASK:
                        TlsHandshake.runDelegatedTasks(engine);
                        continue;
                    case NEED_WRAP:
                        // the encoder sends the handshake data and wakes us up once it needs the peer's answer
                        channel.outboundPipeline().wakeup();
                        return CLEAN;
                    default:
                        break;
                }
                if (!src.hasRemaining()) {
                    return CLEAN;
                }
                plain.clear();
                SSLEngineResult result = engine.unwrap(src, plain);
                plain.flip();
                checkNoRenegotiation(result);
                switch (result.getStatus()) {
                    case OK:
                        if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                            // for authentication by client certificate
                            channel.attributeMap().put(SSLSession.class, engine.getSession());
                            // application data the encoder holds back can go now
                            channel.outboundPipeline().wakeup();
                        }
                        break;
                    case BUFFER_UNDERFLOW:
                        // an incomplete record: wait for more bytes
                        checkSrcCapacity();
                        return CLEAN;
                    case BUFFER_OVERFLOW:
                        plain = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
                        plain.flip();
                        break;
                    case CLOSED:
                        throw new EOFException("The peer closed the TLS connection");
                    default:
                        throw new IllegalStateException("Unexpected " + result);
                }
            }
        } finally {
            compactOrClear(src);
        }
    }

    /**
     * Refuses a new TLS 1.2 handshake started by the connecting side after the first one
     * (client-initiated renegotiation), which costs the member work for nothing.
     */
    private void checkNoRenegotiation(SSLEngineResult result) throws SSLException {
        HandshakeStatus status = result.getHandshakeStatus();
        // the first handshake is done once either handler stored the session (the encoder may finish it)
        boolean handshakeDone = channel.attributeMap().containsKey(SSLSession.class);
        if (handshakeDone && !engine.getUseClientMode() && status != HandshakeStatus.NOT_HANDSHAKING
                && status != HandshakeStatus.FINISHED && "TLSv1.2".equals(engine.getSession().getProtocol())) {
            throw new SSLException("Renegotiation is not allowed");
        }
    }

    /** Moves decrypted bytes to the next handler; returns whether all of them fitted. */
    private boolean passOn() {
        if (!plain.hasRemaining()) {
            return true;
        }
        int count = Math.min(plain.remaining(), dst.remaining());
        if (count > 0) {
            ByteBuffer slice = plain.duplicate();
            slice.limit(plain.position() + count);
            dst.put(slice);
            plain.position(plain.position() + count);
        }
        return !plain.hasRemaining();
    }

    @Override
    public void interceptError(Throwable error) throws Throwable {
        if (error instanceof SSLException) {
            throw new SSLException("TLS failure on " + channel + ": " + error.getMessage(), error);
        }
    }

    private void checkSrcCapacity() {
        // the source is the pipeline's receive buffer, so it cannot be replaced by a bigger one
        int packetSize = engine.getSession().getPacketBufferSize();
        if (src.capacity() < packetSize) {
            throw new IllegalStateException("The receive buffer of " + channel + " (" + src.capacity()
                    + " bytes) is smaller than a TLS record (" + packetSize + " bytes)");
        }
    }
}
