/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.cp.internal.session;

import uk.org.hecaton.client.impl.ClientDelegatingFuture;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPSessionCloseSessionCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPSessionCreateSessionCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.session.AbstractProxySessionManager;
import uk.org.hecaton.cp.internal.session.SessionResponse;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client-side implementation of Raft proxy session manager
 */
public class ClientProxySessionManager extends AbstractProxySessionManager {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;
    private static final long SHUTDOWN_WAIT_SLEEP_MILLIS = 10;

    private final HecatonClientInstanceImpl client;

    public ClientProxySessionManager(HecatonClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    protected long generateThreadId(RaftGroupId groupId) {
        ClientMessage request = CPSessionGenerateThreadIdCodec.encodeRequest(groupId);
        ClientMessage response = new ClientInvocation(client, request, "sessionManager").invoke().joinInternal();

        return CPSessionGenerateThreadIdCodec.decodeResponse(response);
    }

    @Override
    protected SessionResponse requestNewSession(RaftGroupId groupId) {
        ClientMessage request = CPSessionCreateSessionCodec.encodeRequest(groupId, client.getName());
        ClientMessage response = new ClientInvocation(client, request, "sessionManager").invoke().joinInternal();
        CPSessionCreateSessionCodec.ResponseParameters params = CPSessionCreateSessionCodec.decodeResponse(response);
        return new SessionResponse(params.sessionId, params.ttlMillis, params.heartbeatMillis);
    }

    @Override
    protected ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit) {
        return client.getTaskScheduler().scheduleWithRepetition(task, period, period, unit);
    }

    @Override
    protected InternalCompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId) {
        ClientMessage request = CPSessionHeartbeatSessionCodec.encodeRequest(groupId, sessionId);
        ClientInvocationFuture future = new ClientInvocation(client, request, "sessionManager").invoke();
        return new ClientDelegatingFuture<>(future, client.getSerializationService(), clientMessage -> null);
    }

    @Override
    protected InternalCompletableFuture<Object> closeSession(RaftGroupId groupId, Long sessionId) {
        ClientMessage request = CPSessionCloseSessionCodec.encodeRequest(groupId, sessionId);
        ClientInvocationFuture future = new ClientInvocation(client, request, "sessionManager").invoke();
        return new ClientDelegatingFuture<>(future, client.getSerializationService(), CPSessionCloseSessionCodec::decodeResponse);
    }

    public void shutdownAndAwait() {
        Map<RaftGroupId, InternalCompletableFuture<Object>> futures = super.shutdown();

        ILogger logger = client.getLoggingService().getLogger(getClass());

        long remainingTimeNanos = TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);

        while (remainingTimeNanos > 0) {
            int closed = 0;

            for (Entry<RaftGroupId, InternalCompletableFuture<Object>> entry : futures.entrySet()) {
                CPGroupId groupId = entry.getKey();
                InternalCompletableFuture<Object> f = entry.getValue();
                if (f.isDone()) {
                    closed++;
                    try {
                        f.get();
                        logger.fine("Session closed for " + groupId);
                    } catch (Exception e) {
                        logger.warning("Close session failed for " + groupId, e);

                    }
                }
            }

            if (closed == futures.size()) {
                break;
            }

            try {
                Thread.sleep(SHUTDOWN_WAIT_SLEEP_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            remainingTimeNanos -= MILLISECONDS.toNanos(SHUTDOWN_WAIT_SLEEP_MILLIS);
        }
    }

}
