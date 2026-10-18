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

import uk.org.hecaton.client.impl.protocol.MessageTaskFactory;
import uk.org.hecaton.client.impl.protocol.MessageTaskFactoryProvider;
import uk.org.hecaton.client.impl.protocol.codec.JetAddJobStatusListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetExistsDistributedObjectCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetExportSnapshotCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobAndSqlSummaryListCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobIdsCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobMetricsCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobStatusCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobSubmissionTimeCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobSummaryListCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobSuspensionCauseCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetIsJobUserCancelledCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetJoinSubmittedJobCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetRemoveJobStatusListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetResumeJobCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetSubmitJobCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetTerminateJobCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetUpdateJobConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetUploadJobMetaDataCodec;
import uk.org.hecaton.client.impl.protocol.codec.JetUploadJobMultipartCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.util.collection.Int2ObjectHashMap;
import uk.org.hecaton.spi.impl.NodeEngine;

public class JetMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private final Int2ObjectHashMap<MessageTaskFactory> factories = new Int2ObjectHashMap<>(50);
    private final Node node;

    public JetMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = nodeEngine.getNode();
        initFactories();
    }

    public void initFactories() {
        factories.put(JetSubmitJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetSubmitJobMessageTask(cm, node, con));
        factories.put(JetTerminateJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetTerminateJobMessageTask(cm, node, con));
        factories.put(JetGetJobStatusCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobStatusMessageTask(cm, node, con));
        factories.put(JetGetJobSuspensionCauseCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobSuspensionCauseMessageTask(cm, node, con));
        factories.put(JetGetJobMetricsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobMetricsMessageTask(cm, node, con));
        factories.put(JetGetJobIdsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobIdsMessageTask(cm, node, con));
        factories.put(JetJoinSubmittedJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetJoinSubmittedJobMessageTask(cm, node, con));
        factories.put(JetGetJobSubmissionTimeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobSubmissionTimeMessageTask(cm, node, con));
        factories.put(JetGetJobConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobConfigMessageTask(cm, node, con));
        factories.put(JetUpdateJobConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetUpdateJobConfigMessageTask(cm, node, con));
        factories.put(JetResumeJobCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetResumeJobMessageTask(cm, node, con));
        factories.put(JetExportSnapshotCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetExportSnapshotMessageTask(cm, node, con));
        factories.put(JetGetJobSummaryListCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobSummaryListMessageTask(cm, node, con));
        factories.put(JetGetJobAndSqlSummaryListCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetGetJobAndSqlSummaryListMessageTask(cm, node, con));
        factories.put(JetExistsDistributedObjectCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetExistsDistributedObjectMessageTask(cm, node, con));
        factories.put(JetIsJobUserCancelledCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetIsJobUserCancelledMessageTask(cm, node, con));
        factories.put(JetUploadJobMetaDataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetUploadJobMetaDataTask(cm, node, con));
        factories.put(JetUploadJobMultipartCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetUploadJobMultipartTask(cm, node, con));
        factories.put(JetAddJobStatusListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetAddJobStatusListenerMessageTask(cm, node, con));
        factories.put(JetRemoveJobStatusListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new JetRemoveJobStatusListenerMessageTask(cm, node, con));
    }

    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}
