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

package uk.org.hecaton.client.impl.protocol.task.cache;

import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.cache.impl.journal.CacheEventJournalSubscribeOperation;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheEventJournalSubscribeCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.journal.EventJournalInitialSubscriberState;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;


/**
 * Performs the initial subscription to the cache event journal.
 * This includes retrieving the event journal sequences of the
 * oldest and newest event in the journal.
 *
 * @see uk.org.hecaton.cache.impl.CacheProxy#subscribeToEventJournal
 * @see CacheEventJournalSubscribeOperation
 * @since 3.9
 */
public class CacheEventJournalSubscribeTask
        extends AbstractCacheMessageTask<String> {

    public CacheEventJournalSubscribeTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheEventJournalSubscribeOperation(parameters);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return CacheEventJournalSubscribeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        final EventJournalInitialSubscriberState state = (EventJournalInitialSubscriberState) response;
        return CacheEventJournalSubscribeCodec.encodeResponse(state.getOldestSequence(), state.getNewestSequence());
    }

    @Override
    public final String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SUBSCRIBE_TO_EVENT_JOURNAL;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{getPartitionId()};
    }
}
