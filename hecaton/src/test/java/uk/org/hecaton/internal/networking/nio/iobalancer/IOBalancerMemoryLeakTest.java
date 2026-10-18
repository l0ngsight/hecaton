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

package uk.org.hecaton.internal.networking.nio.iobalancer;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.internal.ascii.HTTPCommunicator;
import uk.org.hecaton.internal.networking.nio.NioNetworking;
import uk.org.hecaton.internal.nio.Protocols;
import uk.org.hecaton.internal.server.tcp.TcpServer;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Socket;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class IOBalancerMemoryLeakTest extends HecatonTestSupport {

    @Before
    @After
    public void killAllHecatonInstances() {
        HecatonInstanceFactory.terminateAll();
    }

    @Test
    public void testMemoryLeak_with_RestConnections() throws IOException {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getRestApiConfig().setEnabled(true);
        config.setProperty(ClusterProperty.IO_BALANCER_INTERVAL_SECONDS.getName(), "1");

        HecatonInstance instance = Hecaton.newHecatonInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        for (int i = 0; i < 100; i++) {
            communicator.getClusterInfo();
        }
        System.gc();

        final IOBalancer ioBalancer = getIoBalancer(instance);
        assertTrueEventually(() -> {
            int inPipelineSize = ioBalancer.getInLoadTracker().getPipelines().size();
            int outPipelineSize = ioBalancer.getOutLoadTracker().getPipelines().size();
            assertEquals(0, inPipelineSize);
            assertEquals(0, outPipelineSize);
        });
    }

    @Test
    public void testMemoryLeak_with_SocketConnections() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.setProperty(ClusterProperty.IO_BALANCER_INTERVAL_SECONDS.getName(), "1");
        HecatonInstance instance = Hecaton.newHecatonInstance(config);
        final Address address = instance.getCluster().getLocalMember().getAddress();
        int threadCount = 10;
        final int connectionCountPerThread = 100;

        Runnable runnable = () -> {
            for (int i = 0; i < connectionCountPerThread; i++) {

                try (Socket socket = new Socket(address.getHost(), address.getPort())) {
                    socket.getOutputStream().write(Protocols.CLUSTER.getBytes());
                    sleepMillis(1000);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(runnable);
            threads[i].start();
        }

        assertJoinable(threads);

        final IOBalancer ioBalancer = getIoBalancer(instance);
        assertTrueEventually(() -> {
            LoadTracker inLoadTracker = ioBalancer.getInLoadTracker();
            LoadTracker outLoadTracker = ioBalancer.getOutLoadTracker();
            int inPipelineSize = inLoadTracker.getPipelines().size();
            int outPipelineSize = outLoadTracker.getPipelines().size();
            int inLoadCount = inLoadTracker.getPipelineLoadCount().keySet().size();
            int outLoadCount = outLoadTracker.getPipelineLoadCount().keySet().size();
            int inLastLoadCount = inLoadTracker.getLastLoadCounter().keySet().size();
            int outLastLoadCount = outLoadTracker.getLastLoadCounter().keySet().size();
            assertEquals(0, inPipelineSize);
            assertEquals(0, outPipelineSize);
            assertEquals(0, inLoadCount);
            assertEquals(0, outLoadCount);
            assertEquals(0, inLastLoadCount);
            assertEquals(0, outLastLoadCount);
        });
    }

    private static IOBalancer getIoBalancer(HecatonInstance instance) {
        TcpServer ns = (TcpServer) getNode(instance).getServer();
        NioNetworking threadingModel = (NioNetworking) ns.getNetworking();
        return threadingModel.getIOBalancer();
    }
}
