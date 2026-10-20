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

import uk.org.hecaton.cache.impl.CacheKeysWithCursor;
import uk.org.hecaton.cache.impl.CacheOperationProvider;
import uk.org.hecaton.cache.impl.operation.CacheFetchKeysOperation;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheIterateCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collections;

import static uk.org.hecaton.internal.iteration.IterationPointer.decodePointers;
import static uk.org.hecaton.internal.iteration.IterationPointer.encodePointers;

/**
 * This client request  specifically calls {@link CacheFetchKeysOperation} on the server side.
 *
 * @see CacheFetchKeysOperation
 */
public class CacheIterateMessageTask
        extends AbstractCacheMessageTask<CacheIterateCodec.RequestParameters> {

    public CacheIterateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        IterationPointer[] pointers = decodePointers(parameters.iterationPointers);
        return operationProvider.createFetchKeysOperation(pointers, parameters.batch);
    }

    @Override
    protected CacheIterateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheIterateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response == null) {
            return CacheIterateCodec.encodeResponse(Collections.emptyList(), Collections.emptyList());
        }
        CacheKeysWithCursor keyIteratorResult = (CacheKeysWithCursor) response;
        IterationPointer[] pointers = keyIteratorResult.getPointers();
        return CacheIterateCodec.encodeResponse(encodePointers(pointers), keyIteratorResult.getKeys());
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

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ);
    }
}
