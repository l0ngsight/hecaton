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

package uk.org.hecaton.internal.nio;

/**
 * First bytes to send a node that defines selected protocol
 */
public final class Protocols {

    /**
     * The length in bytes of the protocol-string
     */
    public static final int PROTOCOL_LENGTH = 3;

    /**
     * Protocol that is used among nodes. It differs from Hazelcast's ({@link #HAZELCAST_CLUSTER})
     * so that Hazelcast members cannot join a Hecaton cluster (spec NG1).
     */
    public static final String CLUSTER = "HCC";

    /**
     * Hazelcast's member protocol. Connections that start with it are refused with
     * {@link #HAZELCAST_MEMBER_REJECTED}.
     */
    public static final String HAZELCAST_CLUSTER = "HZC";

    /**
     * Why a connection from a Hazelcast member is refused.
     */
    public static final String HAZELCAST_MEMBER_REJECTED = "A Hazelcast member tried to connect. Hecaton members "
            + "cannot form a cluster with Hazelcast members: run a separate Hecaton cluster and copy the data to it. "
            + "Hazelcast clients can connect to Hecaton.";

    /**
     * New Client Protocol that is used for clients (Java, c++, c# client)
     */
    public static final String CLIENT_BINARY = "CP2";

    /**
     * Protocol that is used by REST
     */
    public static final String REST = "HTTP";

    /**
     * Protocol that is used by Memcached
     */
    public static final String MEMCACHE = "Memcached";

    /**
     * Protocol that is only used if received an unexpected protocol
     */
    public static final String UNEXPECTED_PROTOCOL = "HZX";

    private Protocols() {
    }

    public static String toUserFriendlyString(String protocol) {
        if (CLUSTER.equals(protocol)) {
            return "Cluster Protocol";
        }

        if (CLIENT_BINARY.equals(protocol)) {
            return "Client Open Binary Protocol";
        }

        if (REST.equals(protocol)) {
            return "REST Protocol";
        }

        if (MEMCACHE.equals(protocol)) {
            return "MEMCACHE Protocol";
        }

        return "Unknown Protocol";
    }
}
