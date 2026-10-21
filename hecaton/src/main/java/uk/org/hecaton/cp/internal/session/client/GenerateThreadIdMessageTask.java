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

package uk.org.hecaton.cp.internal.session.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.session.operation.GenerateThreadIdOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;

/**
 * Client message task for {@link GenerateThreadIdOp}
 */
public class GenerateThreadIdMessageTask extends AbstractSessionMessageTask<RaftGroupId, Long> {

    public GenerateThreadIdMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    CPGroupId getGroupId() {
        return parameters;
    }

    @Override
    RaftOp getRaftOp() {
        return new GenerateThreadIdOp();
    }

    @Override
    protected RaftGroupId decodeClientMessage(ClientMessage clientMessage) {
        return CPSessionGenerateThreadIdCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSessionGenerateThreadIdCodec.encodeResponse((Long) response);
    }
}
