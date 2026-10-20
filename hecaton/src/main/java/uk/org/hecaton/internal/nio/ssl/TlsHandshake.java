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
package uk.org.hecaton.internal.nio.ssl;

import uk.org.hecaton.internal.networking.Channel;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Handshake helpers shared by {@link TlsDecoder} and {@link TlsEncoder}, and the handshake
 * timeout.
 */
final class TlsHandshake {

    private static final ILogger LOGGER = Logger.getLogger(TlsHandshake.class);

    private TlsHandshake() {
    }

    /**
     * Closes {@code channel} if the handshake of {@code engine} (which must have begun) is not
     * done after {@code timeoutMillis}, so that a peer cannot hold a connection open without
     * completing it.
     */
    static void closeIfNotDoneWithin(Channel channel, SSLEngine engine, long timeoutMillis) {
        Timer.EXECUTOR.schedule(() -> {
            if (!channel.isClosed() && engine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING) {
                LOGGER.warning("Closing " + channel + ": the TLS handshake did not complete within "
                        + timeoutMillis + " ms");
                try {
                    channel.close();
                } catch (IOException e) {
                    LOGGER.fine("Closing " + channel + " failed", e);
                }
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /** One daemon thread for all handshake timeouts, created on first use. */
    private static final class Timer {
        static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "hc.tls-handshake-timeout");
            thread.setDaemon(true);
            return thread;
        });

        static {
            EXECUTOR.setRemoveOnCancelPolicy(true);
        }
    }

    /**
     * Runs the engine's delegated tasks (certificate checks and the like) on the calling IO
     * thread. They are short, and the connection cannot make progress until they are done.
     */
    static void runDelegatedTasks(SSLEngine engine) {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }
}
