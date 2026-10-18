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

package uk.org.hecaton.internal.cluster.impl.operations;

import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.cluster.impl.ClusterStateManager;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.UrgentSystemOperation;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.UUID;

public class RollbackClusterStateOp extends Operation implements AllowedDuringPassiveState, UrgentSystemOperation,
        IdentifiedDataSerializable {

    private Address initiator;
    private UUID txnId;

    private boolean response;

    public RollbackClusterStateOp() {
    }

    public RollbackClusterStateOp(Address initiator, UUID txnId) {
        this.initiator = initiator;
        this.txnId = txnId;
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl service = getService();
        ClusterStateManager clusterStateManager = service.getClusterStateManager();
        getLogger().info("Rolling back cluster state! Initiator: " + initiator);
        response = clusterStateManager.rollbackClusterState(txnId);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(initiator);
        UUIDSerializationUtil.writeUUID(out, txnId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        initiator = in.readObject();
        txnId = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.ROLLBACK_CLUSTER_STATE;
    }
}
