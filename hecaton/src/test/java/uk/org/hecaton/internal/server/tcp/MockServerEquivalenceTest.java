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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.TestEnvironment;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.RestoreSystemProperties;

import static uk.org.hecaton.instance.EndpointQualifier.MEMBER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks whether the mock network is compatible with the real network behavior.
 * @implNote This test cannot be ParallelJVMTest because it uses the real network.
 */
class MockServerEquivalenceTest {

    private TestHecatonInstanceFactory factory;

    @AfterEach
    void tearDown() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @RestoreSystemProperties
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void transmitShouldRejectLocalPacket(boolean realNetwork) {
        System.setProperty(TestEnvironment.HECATON_TEST_USE_NETWORK, String.valueOf(realNetwork));

        // TestHecatonInstanceFactory depends on HECATON_TEST_USE_NETWORK setting
        factory = new TestHecatonInstanceFactory();
        HecatonInstance instance = factory.newHecatonInstance();

        var connection = Accessors.getNode(instance).getServer().getConnectionManager(MEMBER);
        Packet packet = new Packet(Accessors.getSerializationService(instance).toBytes("dummy"))
                .setPacketType(Packet.Type.NULL);
        assertThat(connection.transmit(packet, Accessors.getAddress(instance))).isFalse();
    }
}
