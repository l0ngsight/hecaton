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

import uk.org.hecaton.internal.networking.Channel;
import uk.org.hecaton.internal.networking.ChannelInitializer;
import uk.org.hecaton.internal.networking.ChannelOptions;
import uk.org.hecaton.internal.server.ServerContext;
import uk.org.hecaton.spi.properties.HecatonProperties;

import static uk.org.hecaton.internal.networking.ChannelOption.DIRECT_BUF;
import static uk.org.hecaton.internal.networking.ChannelOption.SO_KEEPALIVE;
import static uk.org.hecaton.internal.networking.ChannelOption.SO_LINGER;
import static uk.org.hecaton.internal.networking.ChannelOption.SO_RCVBUF;
import static uk.org.hecaton.internal.networking.ChannelOption.SO_SNDBUF;
import static uk.org.hecaton.internal.networking.ChannelOption.TCP_KEEPCOUNT;
import static uk.org.hecaton.internal.networking.ChannelOption.TCP_KEEPIDLE;
import static uk.org.hecaton.internal.networking.ChannelOption.TCP_KEEPINTERVAL;
import static uk.org.hecaton.internal.networking.ChannelOption.TCP_NODELAY;
import static uk.org.hecaton.internal.server.ServerContext.KILO_BYTE;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_BUFFER_DIRECT;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_KEEP_ALIVE;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_KEEP_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_KEEP_IDLE;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_KEEP_INTERVAL;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_LINGER_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_NO_DELAY;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_RECEIVE_BUFFER_SIZE;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_SEND_BUFFER_SIZE;

/**
 * A {@link ChannelInitializer} that runs on a member and used for unencrypted
 * channels. It will deal with the exchange of protocols and based on that it
 * will set up the appropriate handlers in the pipeline.
 */
public class UnifiedChannelInitializer
        implements ChannelInitializer {

    private final ServerContext serverContext;
    private final HecatonProperties props;

    public UnifiedChannelInitializer(ServerContext serverContext) {
        this.props = serverContext.properties();
        this.serverContext = serverContext;
    }

    @Override
    public void initChannel(Channel channel) {
        ChannelOptions config = channel.options();
        config.setOption(DIRECT_BUF, props.getBoolean(SOCKET_BUFFER_DIRECT))
                .setOption(TCP_NODELAY, props.getBoolean(SOCKET_NO_DELAY))
                .setOption(SO_KEEPALIVE, props.getBoolean(SOCKET_KEEP_ALIVE))
                .setOption(SO_SNDBUF, props.getInteger(SOCKET_SEND_BUFFER_SIZE) * KILO_BYTE)
                .setOption(SO_RCVBUF, props.getInteger(SOCKET_RECEIVE_BUFFER_SIZE) * KILO_BYTE)
                .setOption(SO_LINGER, props.getSeconds(SOCKET_LINGER_SECONDS))
                .setOption(TCP_KEEPIDLE, props.getInteger(SOCKET_KEEP_IDLE))
                .setOption(TCP_KEEPCOUNT, props.getInteger(SOCKET_KEEP_COUNT))
                .setOption(TCP_KEEPINTERVAL, props.getInteger(SOCKET_KEEP_INTERVAL));

        UnifiedProtocolEncoder encoder = new UnifiedProtocolEncoder(serverContext);
        UnifiedProtocolDecoder decoder = new UnifiedProtocolDecoder(serverContext, encoder);

        channel.outboundPipeline().addLast(encoder);
        channel.inboundPipeline().addLast(decoder);
    }
}
