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

import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.internal.serialization.impl.compact.SchemaService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;

/**
 * Sent by the master member to the joining member to replay
 * replications occurred so far.
 * <p>
 * The joining member replays the preparation phase for the replications
 * and puts the schema replications to its local registry with the same
 * status.
 * <p>
 * For already {@link SchemaReplicationStatus#REPLICATED} replications,
 * replaying the preparation phase and then putting the replication with
 * the same status to the local registry is enough to maintain the same
 * status across the cluster.
 * <p>
 * For replications in the {@link SchemaReplicationStatus#PREPARED} status,
 * the joining member will eagerly replay the preparation phases again.
 * When the coordinator members start sending {@link AckSchemaReplicationOperation}
 * to participant members for the {@link SchemaReplicationStatus#PREPARED}
 * schemas, the member list version check in the aforementioned operation
 * will make sure that the coordinator members will retry the acknowledgment
 * operation until the new member becomes one of the participants.
 */
public class SendSchemaReplicationsOperation extends Operation implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private Collection<SchemaReplication> replications;

    public SendSchemaReplicationsOperation() {
    }

    public SendSchemaReplicationsOperation(Collection<SchemaReplication> replications) {
        this.replications = replications;
    }

    @Override
    public void run() {
        MemberSchemaService schemaService = getService();
        schemaService.replayReplications(replications);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeCollection(replications, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        replications = SerializationUtil.readCollection(in);
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
    public int getClassId() {
        return SchemaDataSerializerHook.SEND_SCHEMA_REPLICATIONS_OPERATION;
    }
}
