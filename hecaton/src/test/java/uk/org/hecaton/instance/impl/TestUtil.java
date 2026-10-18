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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.client.Client;
import uk.org.hecaton.cluster.Endpoint;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.util.executor.HecatonManagedThread;
import uk.org.hecaton.jet.retry.IntervalFunction;
import uk.org.hecaton.jet.retry.impl.IntervalFunctions;
import uk.org.hecaton.partition.Partition;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.test.starter.HecatonStarter;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.test.HecatonTestSupport.sleepMillis;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.junit.Assert.fail;

@SuppressWarnings("WeakerAccess")
public final class TestUtil {

    private static final SerializationService SERIALIZATION_SERVICE = new DefaultSerializationServiceBuilder().build();

    private TestUtil() {
    }

    /**
     * Serializes the given object with a default {@link SerializationService}.
     *
     * @param obj the object to serialize
     * @return the serialized {@link Data} instance
     */
    public static Data toData(Object obj) {
        return SERIALIZATION_SERVICE.toData(obj);
    }

    /**
     * Deserializes the given {@link Data} instance with a default {@link SerializationService}.
     *
     * @param data the {@link Data} instance to deserialize
     * @return the deserialized object
     */
    public static Object toObject(Data data) {
        return SERIALIZATION_SERVICE.toObject(data);
    }

    /**
     * Retrieves the {@link Node} from a given Hecaton instance.
     *
     * @param hz the Hecaton instance to retrieve the Node from
     * @return the {@link Node} from the given Hecaton instance
     */
    public static Node getNode(HecatonInstance hz) {
        if (isProxyClass(hz.getClass())) {
            return HecatonStarter.getNode(hz);
        } else {
            HecatonInstanceImpl hecatonInstanceImpl = getHecatonInstanceImpl(hz);
            return hecatonInstanceImpl.node;
        }
    }

    /**
     * Retrieves the {@link HecatonInstanceImpl} from a given Hecaton instance.
     *
     * @param hz the Hecaton instance to retrieve the implementation from
     * @return the {@link HecatonInstanceImpl} of the given Hecaton instance
     * @throws IllegalArgumentException if the {@link HecatonInstanceImpl} could not be retrieved,
     *                                  e.g. when called with a Hecaton client instance
     */
    public static HecatonInstanceImpl getHecatonInstanceImpl(HecatonInstance hz) {
        if (hz instanceof HecatonInstanceImpl impl) {
            return impl;
        } else if (hz instanceof HecatonInstanceProxy proxy) {
            if (proxy.original != null) {
                return proxy.original;
            }
        }
        Class<? extends HecatonInstance> clazz = hz.getClass();
        if (isProxyClass(clazz)) {
            return HecatonStarter.getHecatonInstanceImpl(hz);
        }
        throw new IllegalArgumentException("The given HecatonInstance is not an active HecatonInstanceImpl: " + clazz);
    }

    /**
     * Terminates the given Hecaton instance.
     *
     * @param hz the instance to terminate
     */
    public static void terminateInstance(HecatonInstance hz) {
        hz.getLifecycleService().terminate();
    }

    /**
     * Assigns the partition owners of the given Hecaton instances.
     *
     * @param instances the given Hecaton instances
     */
    public static void warmUpPartitions(HecatonInstance... instances) {
        for (HecatonInstance instance : instances) {
            if (instance == null) {
                continue;
            }
            Endpoint localEndpoint = instance.getLocalEndpoint();
            if (!(localEndpoint instanceof Client)) {
                //trigger partition table arrangement and wait on members first
                warmupPartitions(instance);
            }
        }

        //wait on clients to get partition tables.
        for (HecatonInstance instance : instances) {
            if (instance == null) {
                continue;
            }
            if (instance.getLocalEndpoint() instanceof Client) {
                warmupPartitions(instance);
            }
        }
    }

    /**
     * Assigns the partition owners of the given Hecaton instances.
     *
     * @param instances the given Hecaton instances
     */
    public static void warmUpPartitions(Collection<HecatonInstance> instances) {
        warmUpPartitions(instances.toArray(new HecatonInstance[0]));
    }

    private static void warmupPartitions(HecatonInstance instance) {
        if (instance == null) {
            return;
        }
        final int maxRetryCount = 15;
        IntervalFunction intervalFunction = IntervalFunctions.exponentialBackoffWithCap(10L, 2, 1000L);
        PartitionService ps = instance.getPartitionService();

        for (Partition partition : ps.getPartitions()) {
            int i = 1;
            while (partition.getOwner() == null) {
                if (i > maxRetryCount) {
                    fail("The owner of Partition{partitionId=" + partition.getPartitionId() + "}"
                            + " could not be obtained after " + maxRetryCount + " retries.");
                }
                sleepMillis((int) intervalFunction.waitAfterAttempt(i));
                ++i;
            }
        }
    }

    /**
     * Sets up current thread to be able to support User Code Namespaces for given instance.
     * Useful when namespace-aware methods are invoked not on partition threads
     * but directly in test thread. Should not be invoked from Hecaton instance threads
     * which should have this setup done automatically.
     *
     * @implNote This method is {@link uk.org.hecaton.test.annotation.CompatibilityTest}-aware and sets up
     *           UCN thread locals from appropriate classloader.
     *
     * @param hz the Hecaton instance
     */
    public static void setupNamespacesForCurrentThread(HecatonInstance hz) {
        assert !(Thread.currentThread() instanceof HecatonManagedThread) : "Should not be invoked on Hecaton threads";

        if (isProxyClass(hz.getClass())) {
            // This is a shortcut.
            // Currently, onThreadStart only sets up the NodeEngineThreadLocalContext which is exactly what we need.
            // If it starts doing too much, NodeEngineThreadLocalContext.declareNodeEngineReference
            // from appropriate HecatonAPIDelegatingClassloader will have to be invoked reflectively.
            HecatonStarter.getNode(hz).getNodeExtension().onThreadStart(Thread.currentThread());
        } else {
            NodeEngineThreadLocalContext.declareNodeEngineReference(getNode(hz).getNodeEngine());
        }

    }

    /**
     * Returns the next available port (starting from a base port).
     *
     * @param basePort the starting port
     * @return the next available port
     * @see #isPortAvailable(int)
     */
    public static int getAvailablePort(int basePort) {
        return getAvailablePorts(basePort, 1).get(0);
    }

    /**
     * Returns a list of available ports (starting from a base port).
     *
     * @param basePort  the starting port
     * @param portCount the number of ports to search
     * @return a list of available ports
     * @see #isPortAvailable(int)
     */
    public static List<Integer> getAvailablePorts(int basePort, int portCount) {
        List<Integer> availablePorts = new ArrayList<>();
        int port = basePort;
        for (int i = 0; i < portCount; i++) {
            while (!isPortAvailable(port)) {
                port++;
            }
            availablePorts.add(port++);
        }
        return availablePorts;
    }

    /**
     * Checks the DatagramSocket as well if the port is available in UDP and TCP.
     *
     * @param port the port to check
     * @return {@code true} if the port is available in UDP and TCP, {@code false} otherwise
     */
    public static boolean isPortAvailable(int port) {
        try (ServerSocket ss = new ServerSocket(port)) {
            ss.setReuseAddress(true);

            try (DatagramSocket ds = new DatagramSocket(port)) {
                ds.setReuseAddress(true);
                return true;
            }
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Sets or removes a system property.
     * <p>
     * When the value is {@code null}, the system property will be removed.
     * <p>
     * Avoids a {@link NullPointerException} when calling {@link System#setProperty(String, String)} with a {@code null} value.
     *
     * @param key   property name
     * @param value property value
     * @return the previous string value of the system property, or {@code null} if it did not have one
     */
    public static String setSystemProperty(String key, String value) {
        return value == null ? System.clearProperty(key) : System.setProperty(key, value);
    }

    public static byte[] byteBufferToBytes(ByteBuffer buffer) {
        buffer.flip();
        byte[] requestBytes = new byte[buffer.limit()];
        buffer.get(requestBytes);
        return requestBytes;
    }
}
