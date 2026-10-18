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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.server.NetworkStats;
import uk.org.hecaton.internal.nio.ConnectionListener;
import uk.org.hecaton.internal.server.Server;
import uk.org.hecaton.internal.server.ServerConnectionManager;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.GetOperation;
import uk.org.hecaton.partition.Partition;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_ServerConnectionManagerTest
        extends HecatonTestSupport {


    private static final String EXPECTED_MSG = "NOOP";

    @Test
    public void testInvocation_whenEndpointManagerIsNoop() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();

        String key = generateKeyOwnedBy(hz2);
        Data dataKey = getSerializationService(hz1).toData(key);
        Partition partition = hz1.getPartitionService().getPartition(key);

        Operation op = new GetOperation("test", dataKey);
        InvocationBuilder builder
                = getNodeEngineImpl(hz1).getOperationService()
                           .createInvocationBuilder(MapService.SERVICE_NAME, op, partition.getPartitionId());
        builder.setConnectionManager(new NoopEndpointManager());

        assertThatThrownBy(builder::invoke)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(EXPECTED_MSG);
    }

    class NoopEndpointManager implements ServerConnectionManager {

        @Override
        public Server getServer() {
            return null;
        }

        @Override
        public @Nonnull Collection<ServerConnection> getConnections() {
            return Collections.emptyList();
        }

        @Override
        public int connectionCount(Predicate<ServerConnection> predicate) {
            return 0;
        }

        @Override
        public boolean register(
                Address remoteAddress,
                Address targetAddress,
                Collection<Address> remoteAddressAliases,
                UUID remoteUuid,
                ServerConnection connection,
                int streamId
        ) {
            return false;
        }

        @Override
        public ServerConnection get(@Nonnull Address address, int streamId) {
            return null;
        }

        @Override
        @Nonnull
        public List<ServerConnection> getAllConnections(@Nonnull Address address) {
            return Collections.emptyList();
        }

        @Override
        public ServerConnection getOrConnect(@Nonnull Address address , int streamId) {
            throw new UnsupportedOperationException(EXPECTED_MSG);
        }

        @Override
        public ServerConnection getOrConnect(@Nonnull Address address, boolean silent, int streamId) {
            throw new UnsupportedOperationException(EXPECTED_MSG);
        }

        @Override
        public boolean transmit(Packet packet, Address target, int streamId) {
            return false;
        }

        @Override
        public void addConnectionListener(ConnectionListener listener) {
        }

        @Override
        public void accept(Packet packet) {
        }

        @Override
        public NetworkStats getNetworkStats() {
            return null;
        }
    }
}
