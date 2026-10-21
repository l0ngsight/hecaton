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

package uk.org.hecaton.cp.internal.operation;

import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.RaftServiceDataSerializerHook;
import uk.org.hecaton.cp.internal.RaftSystemOperation;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Returns member priority and the CP groups that this local CP member is the Raft group leader.
 */
public class GetLeadedGroupsOp extends Operation implements RaftSystemOperation, IdentifiedDataSerializable {

    private transient Map.Entry<Integer, Collection<CPGroupId>> entry =
        new AbstractMap.SimpleImmutableEntry<>(0, Collections.emptyList());

    public GetLeadedGroupsOp() {
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        Collection<CPGroupId> groups = service.getLeadedGroups();
        int cpMemberPriority = service.getCPMemberPriority();
        entry = new AbstractMap.SimpleImmutableEntry<>(cpMemberPriority, groups);
    }

    @Override
    public Object getResponse() {
        return entry;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public final String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.GET_LEADED_GROUPS;
    }
}
