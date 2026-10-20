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

package uk.org.hecaton.internal.server.tcp;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nio.Protocols;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * A Hazelcast member opens its connections with Hazelcast's member protocol bytes; a Hecaton
 * member must close such a connection, so that Hazelcast members cannot join (spec NG1).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastMemberRejectedTest extends HecatonTestSupport {

    @After
    public void tearDown() {
        Hecaton.shutdownAll();
    }

    @Test
    public void connectionWithHazelcastMemberProtocolIsClosed() throws Exception {
        Config config = smallInstanceConfig();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        HecatonInstance member = Hecaton.newHecatonInstance(config);
        Address address = member.getCluster().getLocalMember().getAddress();

        try (Socket socket = new Socket(address.getInetAddress(), address.getPort())) {
            socket.setSoTimeout(30_000);
            socket.getOutputStream().write(Protocols.HAZELCAST_CLUSTER.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            InputStream in = socket.getInputStream();
            // the member closes the connection without answering
            assertEquals(-1, in.read());
        }
        assertClusterSize(1, member);
    }
}
