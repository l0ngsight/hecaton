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
import uk.org.hecaton.client.impl.protocol.codec.JetGetJobAndSqlSummaryListCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.jet.impl.JobAndSqlSummary;
import uk.org.hecaton.jet.impl.operation.GetJobAndSqlSummaryListOperation;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.JobPermission;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrowFromCollection;
import static uk.org.hecaton.jet.impl.util.Util.checkJetIsEnabled;
import static uk.org.hecaton.jet.impl.util.Util.distinctBy;

public class JetGetJobAndSqlSummaryListMessageTask extends AbstractMultiTargetMessageTask<Void> {
    private static final Class[] IGNORED_EXCEPTIONS = {TargetNotMemberException.class, MemberLeftException.class};

    JetGetJobAndSqlSummaryListMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return GetJobAndSqlSummaryListOperation::new;
    }

    @Override
    public Collection<Member> getTargets() {
        checkJetIsEnabled(nodeEngine);
        return nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        rethrowFromCollection(map.values(), IGNORED_EXCEPTIONS);
        return map.values().stream()
                .filter(item -> !(item instanceof Throwable))
                .flatMap(item -> ((List<JobAndSqlSummary>) item).stream())
                // In edge cases there can be duplicates. E.g. the GetIdsOp is broadcast to all members.  member1
                // is master and responds and dies. It's removed from cluster, member2 becomes master and
                // responds with the same normal jobs. It's safe to remove duplicates because the same jobId should
                // be the same job - we use FlakeIdGenerator to generate the IDs.
                .filter(distinctBy(JobAndSqlSummary::getJobId))
                .collect(Collectors.toList());
    }

    @Override
    protected Void decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetGetJobAndSqlSummaryListCodec.encodeResponse((List<JobAndSqlSummary>) response);
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getJobAndSqlSummaryList";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public String getServiceName() {
        return JetServiceBackend.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new JobPermission(ActionConstants.ACTION_READ);
    }
}
