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

import uk.org.hecaton.config.AdvancedNetworkConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EndpointConfig;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.internal.networking.ChannelInitializer;
import uk.org.hecaton.internal.nio.ascii.TextChannelInitializer;
import uk.org.hecaton.internal.nio.ssl.SSLEngineFactories;
import uk.org.hecaton.internal.nio.ssl.TlsChannelInitializer;
import uk.org.hecaton.internal.server.ServerContext;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.nio.ssl.SSLEngineFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ChannelInitializerFunction implements Function<EndpointQualifier, ChannelInitializer> {
    protected static final ILogger LOGGER = Logger.getLogger(ChannelInitializerFunction.class);

    protected final ServerContext serverContext;
    private final ChannelInitializer uniChannelInitializer;
    private final Config config;
    private volatile Map<EndpointQualifier, ChannelInitializer> initializerMap;

    public ChannelInitializerFunction(ServerContext serverContext, Config config) {
        this.serverContext = serverContext;
        this.config = config;
        ChannelInitializer unified = new UnifiedChannelInitializer(serverContext);
        // unisocket: one SSL configuration for all connections
        this.uniChannelInitializer = config.getAdvancedNetworkConfig().isEnabled()
                ? unified
                : withTls(unified, config.getNetworkConfig().getSSLConfig());
    }

    @Override
    public ChannelInitializer apply(EndpointQualifier qualifier) {
        return initializerMap.isEmpty() ? provideUnifiedChannelInitializer() : initializerMap.get(qualifier);
    }

    public void init() {
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        if (!advancedNetworkConfig.isEnabled() || advancedNetworkConfig.getEndpointConfigs().isEmpty()) {
            initializerMap = Collections.emptyMap();
            return;
        }

        Map<EndpointQualifier, ChannelInitializer> map = new HashMap<>();
        for (EndpointConfig endpointConfig : advancedNetworkConfig.getEndpointConfigs().values()) {
            logSSLConfig(endpointConfig);

            SSLConfig sslConfig = endpointConfig.getSSLConfig();
            switch (endpointConfig.getProtocolType()) {
                case MEMBER:
                    map.put(EndpointQualifier.MEMBER, withTls(provideMemberChannelInitializer(endpointConfig), sslConfig));
                    break;
                case CLIENT:
                    map.put(EndpointQualifier.CLIENT, withTls(provideClientChannelInitializer(endpointConfig), sslConfig));
                    break;
                case REST:
                    map.put(EndpointQualifier.REST, withTls(provideTextChannelInitializer(endpointConfig, true), sslConfig));
                    break;
                case MEMCACHE:
                    map.put(EndpointQualifier.MEMCACHE,
                            withTls(provideTextChannelInitializer(endpointConfig, false), sslConfig));
                    break;
                case WAN:
                    map.put(endpointConfig.getQualifier(),
                            withTls(provideMemberChannelInitializer(endpointConfig), sslConfig));
                    break;
                default:
                    throw new IllegalStateException("Cannot build channel initializer for protocol type "
                                                    + endpointConfig.getProtocolType());
            }
        }

        initializerMap = map;
    }

    private void logSSLConfig(EndpointConfig endpointConfig) {
        SSLConfig sslConfig = endpointConfig.getSSLConfig();
        if (sslConfig != null && sslConfig.isEnabled()) {
            switch (endpointConfig.getProtocolType()) {
                case MEMBER -> LOGGER.info("SSL is enabled for member-to-member connections");
                case CLIENT -> LOGGER.info("SSL is enabled for client-to-member connections");
                case WAN -> LOGGER.info("SSL is enabled for WAN connections");
                case REST -> LOGGER.info("SSL is enabled for REST connections");
                case MEMCACHE -> LOGGER.info("SSL is enabled for MEMCACHE connections");
                default -> LOGGER.info("Unknown Protocol Type");
            }
        }
    }

    protected ChannelInitializer provideUnifiedChannelInitializer() {
        return uniChannelInitializer;
    }

    protected ChannelInitializer provideMemberChannelInitializer(EndpointConfig endpointConfig) {
        return new MemberChannelInitializer(serverContext, endpointConfig);
    }

    protected ChannelInitializer provideClientChannelInitializer(EndpointConfig endpointConfig) {
        return new ClientChannelInitializer(serverContext, endpointConfig);
    }

    protected ChannelInitializer provideTextChannelInitializer(EndpointConfig endpointConfig, boolean rest) {
        return new TextChannelInitializer(serverContext, endpointConfig, rest);
    }

    protected ChannelInitializer provideWanChannelInitializer(EndpointConfig endpointConfig) {
        throw new UnsupportedOperationException("TODO");
    }

    /** Adds TLS (F1) to the channels of {@code initializer} if {@code sslConfig} enables it. */
    private ChannelInitializer withTls(ChannelInitializer initializer, SSLConfig sslConfig) {
        if (sslConfig == null || !sslConfig.isEnabled()) {
            return initializer;
        }
        SSLEngineFactory engineFactory = SSLEngineFactories.create(sslConfig, false, config.getClassLoader());
        return new TlsChannelInitializer(initializer, engineFactory,
                SSLEngineFactories.handshakeTimeoutMillis(sslConfig));
    }
}
