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

import uk.org.hecaton.cache.impl.CacheEntriesWithCursor;
import uk.org.hecaton.cache.impl.CacheOperationProvider;
import uk.org.hecaton.cache.impl.operation.CacheFetchEntriesOperation;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheIterateEntriesCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collections;

import static uk.org.hecaton.internal.iteration.IterationPointer.decodePointers;
import static uk.org.hecaton.internal.iteration.IterationPointer.encodePointers;

/**
 * This client request specifically calls {@link CacheFetchEntriesOperation} on the server side.
 *
 * @see CacheFetchEntriesOperation
 */
public class CacheIterateEntriesMessageTask
        extends AbstractCacheMessageTask<CacheIterateEntriesCodec.RequestParameters> {

    public CacheIterateEntriesMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        return operationProvider.createFetchEntriesOperation(
                decodePointers(parameters.iterationPointers), parameters.batch);
    }

    @Override
    protected CacheIterateEntriesCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheIterateEntriesCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response == null) {
            return CacheIterateEntriesCodec.encodeResponse(Collections.emptyList(), Collections.emptyList());
        }
        CacheEntriesWithCursor iteratorResult = (CacheEntriesWithCursor) response;
        IterationPointer[] pointers = iteratorResult.getPointers();
        return CacheIterateEntriesCodec.encodeResponse(
                encodePointers(pointers), iteratorResult.getEntries());
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ITERATOR;
    }
}
