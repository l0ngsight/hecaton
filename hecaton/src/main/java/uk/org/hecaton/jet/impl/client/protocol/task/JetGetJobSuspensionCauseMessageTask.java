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

package uk.org.hecaton.jet.impl.client.protocol.task;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobSuspensionCauseCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.jet.impl.operation.GetJobSuspensionCauseOperation;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;

public class JetGetJobSuspensionCauseMessageTask extends AbstractJetMessageTask<Long, Data> {

    JetGetJobSuspensionCauseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                JetGetJobSuspensionCauseCodec::decodeRequest,
                JetGetJobSuspensionCauseCodec::encodeResponse);
    }

    @Override
    protected ClientMessage encodeResponse(Object o) {
        return super.encodeResponse(toData(o));
    }

    @Override
    protected Operation prepareOperation() {
        return new GetJobSuspensionCauseOperation(parameters);
    }

    @Override
    public String getMethodName() {
        return "getJobSuspensionCause";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Nullable
    @Override
    public String[] actions() {
        return new String[]{ActionConstants.ACTION_READ};
    }
}
