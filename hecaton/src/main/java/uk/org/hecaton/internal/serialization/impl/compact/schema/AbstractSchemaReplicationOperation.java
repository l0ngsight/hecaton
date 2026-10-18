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

package uk.org.hecaton.internal.serialization.impl.compact.schema;

import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.cluster.impl.ClusterTopologyChangedException;
import uk.org.hecaton.internal.serialization.impl.compact.SchemaService;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.spi.impl.operationservice.Operation;

/**
 * Contains the common components of the all schema replication operations.
 */
public abstract class AbstractSchemaReplicationOperation extends Operation implements IdentifiedDataSerializable {

    // Each schema replication operation is guarded by the member list version
    // checks so that the operations can be retried when a joining member is
    // detected on the master
    protected int memberListVersion;

    protected AbstractSchemaReplicationOperation() {
    }

    protected AbstractSchemaReplicationOperation(int memberListVersion) {
        this.memberListVersion = memberListVersion;
    }

    protected abstract void runInternal();

    @Override
    public void run() throws Exception {
        runInternal();
        checkReceivedMemberListVersion();
    }

    private void checkReceivedMemberListVersion() {
        ClusterService clusterService = getNodeEngine().getClusterService();
        if (!clusterService.isMaster()) {
            // No need to check for the version in non-master members, as the
            // master will be the one that is aware of the joining members first
            return;
        }

        int currentMemberListVersion = clusterService.getMemberListVersion();
        if (currentMemberListVersion == memberListVersion) {
            // All good, the member list on the master and the initiator are
            // the same. If a member joins after this line, it is fine, as the
            // proper replaying of replications will be handled by the pre-join
            // operations.
            return;
        }

        // Throw the exception to notify the initiator about the
        // newly joining member.
        throw new ClusterTopologyChangedException("Current member list version " + currentMemberListVersion
                + " does not match expected " + memberListVersion);
    }

    @Override
    public String getServiceName() {
        return SchemaService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SchemaDataSerializerHook.F_ID;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof ClusterTopologyChangedException) {
            // Throw it directly to the caller, so that the operation can
            // be retried with the new member list
            return ExceptionAction.THROW_EXCEPTION;
        }

        return super.onInvocationException(throwable);
    }
}
