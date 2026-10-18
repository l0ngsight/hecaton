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

package uk.org.hecaton.client.impl.proxy;

import uk.org.hecaton.client.HecatonClientNotActiveException;
import uk.org.hecaton.client.HecatonClientOfflineException;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.topic.ReliableMessageListener;
import uk.org.hecaton.topic.impl.reliable.MessageRunner;
import uk.org.hecaton.topic.impl.reliable.ReliableTopicMessage;
import uk.org.hecaton.version.MemberVersion;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static uk.org.hecaton.internal.util.ExceptionUtil.peel;

/**
 * Client implementation of  {@link uk.org.hecaton.topic.impl.reliable.MessageRunner}
 *
 * @param <E> message type
 */
public class ClientReliableMessageRunner<E> extends MessageRunner<E> {

    ClientReliableMessageRunner(UUID id, ReliableMessageListener<E> listener,
                                Ringbuffer<ReliableTopicMessage> ringbuffer, String topicName,
                                int batchSze, SerializationService serializationService,
                                Executor executor, ConcurrentMap<UUID, MessageRunner<E>> runnersMap,
                                ILogger logger) {
        super(id, listener, ringbuffer, topicName, batchSze, serializationService, executor, runnersMap, logger);
    }

    @Override
    protected void runWithNamespaceAwareness(Runnable runnable) {
        runnable.run();
    }

    @Override
    protected Member getMember(ReliableTopicMessage m) {
        Member member = null;
        if (m.getPublisherAddress() != null) {
            member = new MemberImpl(m.getPublisherAddress(), MemberVersion.UNKNOWN, false);
        }
        return member;
    }

    @Override
    protected boolean handleInternalException(Throwable t) {
        if (t instanceof HecatonClientNotActiveException) {
            logger.finest("Terminating MessageListener %s on topic: %s. Reason: HecatonClient is shutting down", listener,
                    topicName);
            return false;
        } else if (t instanceof HecatonClientOfflineException) {
            if (logger.isFinestEnabled()) {
                logger.finest("MessageListener " + listener + " on topic: " + topicName + " got exception: " + t
                        + ". Continuing from last known sequence: " + sequence);
            }
            return true;
        }
        return super.handleInternalException(t);
    }

    @Override
    protected Throwable adjustThrowable(Throwable t) {
        return peel(t);
    }
}
