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

package uk.org.hecaton.vector.module;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.task.AbstractAsyncMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.VectorCollectionPermission;
import uk.org.hecaton.vector.impl.VectorCollectionServiceUtil;

import java.security.Permission;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A client request on a vector collection: decoded by its codec, run on the member proxy of the
 * collection, answered by its codec.
 *
 * @param <P> the type of the request parameters
 */
final class VectorMessageTask<P> extends AbstractAsyncMessageTask<P, Object> {

    private final Kind<P> kind;

    VectorMessageTask(Kind<P> kind, ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        this.kind = kind;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CompletableFuture<Object> processInternal() {
        VectorCollectionProxy<?, ?> proxy = (VectorCollectionProxy<?, ?>) nodeEngine.getProxyService()
                .getDistributedObject(VectorCollectionServiceUtil.SERVICE_NAME, getDistributedObjectName(),
                        endpoint.getUuid());
        return (CompletableFuture<Object>) kind.call().apply(proxy, parameters);
    }

    @Override
    protected P decodeClientMessage(ClientMessage clientMessage) {
        return kind.decoder().apply(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return kind.encoder().apply(response);
    }

    @Override
    public String getServiceName() {
        return VectorCollectionServiceUtil.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new VectorCollectionPermission(getDistributedObjectName(), kind.action());
    }

    @Override
    public String getDistributedObjectName() {
        return kind.name().apply(parameters);
    }

    @Override
    public String getMethodName() {
        return kind.method();
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    /**
     * What a request does.
     *
     * @param decoder decodes the request
     * @param name    the name of the collection in the request
     * @param action  the permission action the request needs
     * @param method  the name of the method, for the audit log
     * @param call    runs the request
     * @param encoder encodes the response
     */
    record Kind<P>(Function<ClientMessage, P> decoder, Function<P, String> name, String action, String method,
                   BiFunction<VectorCollectionProxy<?, ?>, P, CompletableFuture<?>> call,
                   Function<Object, ClientMessage> encoder) {
    }
}
