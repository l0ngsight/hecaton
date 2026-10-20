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
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.internal.CPMemberInfo;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.RaftServiceDataSerializerHook;
import uk.org.hecaton.cp.internal.RaftSystemOperation;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.CallStatus;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.function.BiConsumer;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;

/**
 * Triggers the local CP member to transfer Raft group leadership to given CP
 * member for the given CP group
 */
public class TransferLeadershipOp extends Operation implements RaftSystemOperation, IdentifiedDataSerializable,
                                                               BiConsumer<Object, Throwable> {

    private CPGroupId groupId;
    private CPMember destination;

    public TransferLeadershipOp() {
    }

    public TransferLeadershipOp(CPGroupId groupId, CPMember destination) {
        this.groupId = groupId;
        this.destination = destination;
    }

    @Override
    public CallStatus call() throws Exception {
        RaftService service = getService();
        InternalCompletableFuture future = service.transferLeadership(groupId, (CPMemberInfo) destination);
        future.whenCompleteAsync(this, CALLER_RUNS);
        return CallStatus.VOID;
    }

    @Override
    public void accept(Object response, Throwable throwable) {
        if (throwable == null) {
            sendResponse(response);
        } else {
            sendResponse(throwable);
        }
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
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
        return RaftServiceDataSerializerHook.TRANSFER_LEADERSHIP_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeObject(destination);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        destination = in.readObject();
    }
}
