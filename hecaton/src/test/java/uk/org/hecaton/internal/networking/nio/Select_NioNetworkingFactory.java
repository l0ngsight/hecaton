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

package uk.org.hecaton.internal.networking.nio;

import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.internal.server.MockServerContext;
import uk.org.hecaton.internal.server.NetworkingFactory;
import uk.org.hecaton.internal.server.tcp.TcpServerConnectionChannelErrorHandler;
import uk.org.hecaton.spi.properties.HecatonProperties;

import static uk.org.hecaton.spi.properties.ClusterProperty.IO_BALANCER_INTERVAL_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.IO_INPUT_THREAD_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.IO_OUTPUT_THREAD_COUNT;

public class Select_NioNetworkingFactory implements NetworkingFactory {

    @Override
    public NioNetworking create(final MockServerContext serverContext, MetricsRegistry metricsRegistry) {
        HecatonProperties properties = serverContext.properties();
        LoggingService loggingService = serverContext.loggingService;
        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(loggingService)
                        .metricsRegistry(metricsRegistry)
                        .threadNamePrefix(serverContext.getHecatonName())
                        .errorHandler(
                                new TcpServerConnectionChannelErrorHandler(
                                        loggingService.getLogger(TcpServerConnectionChannelErrorHandler.class)))
                        .inputThreadCount(properties.getInteger(IO_INPUT_THREAD_COUNT))
                        .outputThreadCount(properties.getInteger(IO_OUTPUT_THREAD_COUNT))
                        .balancerIntervalSeconds(properties.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .selectorMode(SelectorMode.SELECT));
    }
}
