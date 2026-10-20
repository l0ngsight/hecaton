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

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.networking.Channel;
import uk.org.hecaton.internal.networking.ChannelInitializer;
import uk.org.hecaton.nio.ssl.SSLEngineFactory;

import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Adds TLS to the channels another {@link ChannelInitializer} sets up: a {@link TlsDecoder}
 * before its inbound handlers and a {@link TlsEncoder} after its outbound handlers. The
 * handlers in between see plain bytes, and may replace themselves as usual.
 */
public class TlsChannelInitializer implements ChannelInitializer {

    private final ChannelInitializer delegate;
    private final SSLEngineFactory engineFactory;
    private final long handshakeTimeoutMillis;

    /**
     * @param handshakeTimeoutMillis how long a connection may take to complete the handshake
     *                               before it is closed
     */
    public TlsChannelInitializer(ChannelInitializer delegate, SSLEngineFactory engineFactory,
                                 long handshakeTimeoutMillis) {
        this.delegate = delegate;
        this.engineFactory = engineFactory;
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
    }

    @Override
    public void initChannel(Channel channel) throws Exception {
        SSLEngine engine = engineFactory.create(channel.isClientMode(), peerAddress(channel));
        channel.inboundPipeline().addLast(new TlsDecoder(engine));
        delegate.initChannel(channel);
        channel.outboundPipeline().addLast(new TlsEncoder(engine));
        engine.beginHandshake();
        TlsHandshake.closeIfNotDoneWithin(channel, engine, handshakeTimeoutMillis);
    }

    private static Address peerAddress(Channel channel) {
        SocketAddress remote = channel.remoteSocketAddress();
        if (remote instanceof InetSocketAddress address && address.getAddress() != null) {
            // the host name the connection was made to, if any, for hostname validation
            return new Address(address.getHostString(), address.getAddress(), address.getPort());
        }
        return null;
    }
}
