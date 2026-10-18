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
import uk.org.hecaton.client.impl.protocol.codec.JetTerminateJobCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.jet.impl.TerminationMode;
import uk.org.hecaton.jet.impl.TerminationMode.ActionAfterTerminate;
import uk.org.hecaton.jet.impl.operation.TerminateJobOperation;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;

import java.util.UUID;

public class JetTerminateJobMessageTask extends AbstractJetMessageTask<JetTerminateJobCodec.RequestParameters, Void> {

    JetTerminateJobMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection, JetTerminateJobCodec::decodeRequest,
                o -> JetTerminateJobCodec.encodeResponse());
    }

    @Override
    protected UUID getLightJobCoordinator() {
        return parameters.lightJobCoordinator;
    }

    @Override
    protected Operation prepareOperation() {
        return new TerminateJobOperation(parameters.jobId, TerminationMode.values()[parameters.terminateMode],
                parameters.lightJobCoordinator != null);
    }

    @Override
    public String getMethodName() {
        return "terminateJob";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{};
    }

    @Nullable
    @Override
    public String[] actions() {
        ActionAfterTerminate terminationMode = TerminationMode.values()[parameters.terminateMode].actionAfterTerminate();
        switch (terminationMode) {
            case RESTART:
            case SUSPEND:
                return new String[]{ActionConstants.ACTION_RESTART};
            case CANCEL:
                return new String[]{ActionConstants.ACTION_CANCEL};
            default:
                throw new IllegalArgumentException("Unknown action after termination mode " + terminationMode);
        }
    }
}
