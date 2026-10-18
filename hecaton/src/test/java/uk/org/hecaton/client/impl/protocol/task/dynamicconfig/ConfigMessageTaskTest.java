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

package uk.org.hecaton.client.impl.protocol.task.dynamicconfig;

import uk.org.hecaton.client.impl.ClientEndpoint;
import uk.org.hecaton.client.impl.ClientEndpointManager;
import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.protocol.ClientExceptionFactory;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.task.AbstractMessageTask;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.dynamicconfig.ClusterWideConfigurationService;
import uk.org.hecaton.internal.dynamicconfig.ConfigurationService;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.nio.ConnectionType;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.cluster.Address.createUnresolvedAddress;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class ConfigMessageTaskTest<V extends AbstractMessageTask<?>> {
    protected Connection mockConnection;
    protected NodeEngine mockNodeEngine;
    protected ClientEngine mockClientEngine;
    protected Config config;
    protected ILogger logger;
    protected NodeExtension mockNodeExtension;

    @Before
    public void setup() {
        // setup mocks
        mockConnection = mock(ServerConnection.class);
        mockClientEngine = mock(ClientEngine.class, RETURNS_DEEP_STUBS);
        config = new Config();
        logger = Logger.getLogger(getClass());
        ClientEndpointManager mockClientEndpointManager = mock(ClientEndpointManager.class);
        ClientEndpoint mockClientEndpoint = mock(ClientEndpoint.class);
        mockNodeEngine = mock(NodeEngine.class, RETURNS_DEEP_STUBS);
        mockNodeExtension = mock(NodeExtension.class);
        ClusterWideConfigurationService mockConfigurationService = mock(ClusterWideConfigurationService.class);
        InternalCompletableFuture<Object> mockFuture = mock(InternalCompletableFuture.class);

        when(mockClientEngine.getEndpointManager()).thenReturn(mockClientEndpointManager);
        ClientExceptionFactory clientExceptionFactory = new ClientExceptionFactory(false,
                config.getClassLoader(), mock(LoggingService.class));
        when(mockClientEngine.getExceptionFactory()).thenReturn(clientExceptionFactory);
        when(mockClientEngine.getManagementTasksChecker().isTrusted(any())).thenReturn(true);

        when(mockClientEndpoint.getClientType()).thenReturn(ConnectionType.JAVA_CLIENT);
        when(mockClientEndpoint.isAuthenticated()).thenReturn(true);
        when(mockClientEndpointManager.getEndpoint(mockConnection)).thenReturn(mockClientEndpoint);

        when(mockNodeEngine.getConfig())
                .thenReturn(new DynamicConfigurationAwareConfig(config, new HecatonProperties(config)));
        when(mockNodeEngine.getService(ConfigurationService.SERVICE_NAME)).thenReturn(mockConfigurationService);

        when(mockConfigurationService.broadcastConfigAsync(any(IdentifiedDataSerializable.class)))
                .thenReturn(mockFuture);
        when(mockNodeExtension.isStartCompleted()).thenReturn(true);
        when(mockConnection.getRemoteAddress()).thenReturn(createUnresolvedAddress("127.0.0.1", 5701));
    }

    protected abstract V createMessageTask(ClientMessage clientMessage);
}
