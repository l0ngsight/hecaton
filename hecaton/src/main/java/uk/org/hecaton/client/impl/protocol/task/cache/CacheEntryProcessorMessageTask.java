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

import uk.org.hecaton.cache.impl.CacheOperationProvider;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.operation.CacheEntryProcessorOperation;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheEntryProcessorCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import javax.cache.processor.EntryProcessor;
import java.security.Permission;
import java.util.ArrayList;

/**
 * This client request  specifically calls {@link CacheEntryProcessorOperation} on the server side.
 *
 * @see CacheEntryProcessorOperation
 */
public class CacheEntryProcessorMessageTask
        extends AbstractCacheMessageTask<CacheEntryProcessorCodec.RequestParameters> {

    public CacheEntryProcessorMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected Operation prepareOperation() {
        CacheService service = getService(getServiceName());
        CacheOperationProvider operationProvider = getOperationProvider(parameters.name);
        String namespace = CacheService.lookupNamespace(nodeEngine, parameters.name);
        EntryProcessor entryProcessor = (EntryProcessor) service.toObject(parameters.entryProcessor, namespace);
        ArrayList argumentsList = new ArrayList(parameters.arguments.size());
        for (Data data : parameters.arguments) {
            argumentsList.add(service.toObject(data, namespace));
        }
        return operationProvider
                .createEntryProcessorOperation(parameters.key, parameters.completionId, entryProcessor
                        , argumentsList.toArray());
    }

    @Override
    protected CacheEntryProcessorCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheEntryProcessorCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheEntryProcessorCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(parameters.name, ActionConstants.ACTION_READ,
                ActionConstants.ACTION_REMOVE, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.entryProcessor, parameters.arguments};
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.INVOKE;
    }
}
