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

package uk.org.hecaton.client.impl.protocol.task;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractInvocationMessageTask<P>
        extends AbstractAsyncMessageTask<P, Object> {

    protected AbstractInvocationMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Object> processInternal() {
        Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        return getInvocationBuilder(op).setResultDeserialized(false).invoke();
    }

    protected abstract InvocationBuilder getInvocationBuilder(Operation op);

    protected abstract Operation prepareOperation();
}
