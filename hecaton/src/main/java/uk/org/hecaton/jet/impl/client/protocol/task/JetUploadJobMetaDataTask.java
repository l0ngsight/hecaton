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
import uk.org.hecaton.client.impl.protocol.codec.JetUploadJobMetaDataCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.jet.impl.operation.UploadJobMetaDataOperation;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;

/**
 * The task that creates UploadJobMetaDataOperation with client protocol message
 */
public class JetUploadJobMetaDataTask extends
        AbstractJetMessageTask<JetUploadJobMetaDataCodec.RequestParameters, Boolean> {

    protected JetUploadJobMetaDataTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection,
                JetUploadJobMetaDataCodec::decodeRequest,
                encoder -> JetUploadJobMetaDataCodec.encodeResponse());
    }

    @Override
    public String[] actions() {
        return new String[]{ActionConstants.ACTION_SUBMIT, ActionConstants.ACTION_ADD_RESOURCES};
    }


    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        // Most Jet Tasks forward the task to Job Coordinator. In Job Upload case we don't want to forward but process
        // the task on the current member.
        // When the job upload is complete the job definition will be sent to Job Coordinator
        return nodeEngine.getOperationService().createInvocationBuilder(JetServiceBackend.SERVICE_NAME,
                op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new UploadJobMetaDataOperation(this.parameters);
    }

    @Override
    public String getMethodName() {
        return "uploadJobMetaData";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
