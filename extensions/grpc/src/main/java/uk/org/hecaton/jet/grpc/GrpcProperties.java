/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.grpc;

import uk.org.hecaton.jet.grpc.impl.BidirectionalStreamingService;
import uk.org.hecaton.spi.properties.HecatonProperty;
import io.grpc.stub.StreamObserver;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Properties of the grpc module
 *
 * @since Jet 4.2
 */
public final class GrpcProperties {

    /**
     * Time to wait for {@link StreamObserver#onCompleted()} confirmation when destroying a
     * {@link BidirectionalStreamingService}
     * You may want to increase this value when your gRPC calls take longer than 1 s to cleanly shutdown the service.
     * <p>
     * The default value is 1 s
     */
    public static final HecatonProperty DESTROY_TIMEOUT
            = new HecatonProperty("jet.grpc.destroy.timeout.seconds", 1, SECONDS);

    /**
     * Time to wait for clean shutdown of a {@link io.grpc.ManagedChannel}
     * <p>
     * The default value is 1 s
     */
    public static final HecatonProperty SHUTDOWN_TIMEOUT
            = new HecatonProperty("jet.grpc.shutdown.timeout.seconds", 1, SECONDS);

    private GrpcProperties() {
    }
}
