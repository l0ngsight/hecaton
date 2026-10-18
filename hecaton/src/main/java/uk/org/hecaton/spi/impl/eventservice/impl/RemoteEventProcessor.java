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

package uk.org.hecaton.spi.impl.eventservice.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import uk.org.hecaton.internal.util.executor.StripedRunnable;

import static uk.org.hecaton.internal.util.EmptyStatement.ignore;

/**
 * An extension of the {@link EventProcessor} which logs and swallows any exception while processing the event.
 * The {@link #orderKey} for this processor is equal to the packet partition ID. This means that when running
 * inside a {@link uk.org.hecaton.internal.util.executor.StripedExecutor}, all events for the same partition ID will be ordered.
 *
 * @see EventServiceImpl#sendEvent(Address, EventEnvelope, int)
 */
public class RemoteEventProcessor extends EventProcessor implements StripedRunnable {

    private final EventServiceImpl eventService;
    private final Packet packet;

    public RemoteEventProcessor(EventServiceImpl eventService, Packet packet) {
        super(eventService, null, packet.getPartitionId());
        this.eventService = eventService;
        this.packet = packet;
    }

    @Override
    public void run() {
        try {
            EventEnvelope eventEnvelope = eventService.nodeEngine.toObject(packet);
            process(eventEnvelope);
        } catch (ReplicatedMapCantBeCreatedOnLiteMemberException e) {
            // this happens when there is a lite member in the cluster
            // and a data member creates a ReplicatedMap proxy
            // (this is totally expected and doesn't need logging)
            ignore(e);
        } catch (Exception e) {
            eventService.logger.warning("Error while logging processing event", e);
        }
    }
}
