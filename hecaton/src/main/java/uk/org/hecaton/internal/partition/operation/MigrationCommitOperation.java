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

package uk.org.hecaton.internal.partition.operation;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.MigrationCycleOperation;
import uk.org.hecaton.internal.partition.MigrationInfo;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.PartitionDataSerializerHook;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;

import java.io.IOException;
import java.util.UUID;

/**
 * Sent by the master node to commit a migration on the migration destination.
 * It updates the partition table on the migration destination and finalizes the migration.
 */
public class MigrationCommitOperation extends AbstractPartitionOperation implements MigrationCycleOperation {

    private MigrationInfo migration;

    private UUID expectedMemberUuid;

    private transient boolean success;

    public MigrationCommitOperation() {
    }

    public MigrationCommitOperation(MigrationInfo migration, UUID expectedMemberUuid) {
        this.migration = migration;
        this.expectedMemberUuid = expectedMemberUuid;
    }

    @Override
    public void run() {
        NodeEngine nodeEngine = getNodeEngine();
        final Member localMember = nodeEngine.getLocalMember();
        if (!localMember.getUuid().equals(expectedMemberUuid)) {
            throw new IllegalStateException("This " + localMember
                    + " is migration commit destination but most probably it's restarted "
                    + "and not the expected target.");
        }

        InternalPartitionServiceImpl service = getService();
        success = service.commitMigrationOnDestination(migration, getCallerAddress());
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public String getServiceName() {
        return IPartitionService.SERVICE_NAME;
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expectedMemberUuid = UUIDSerializationUtil.readUUID(in);
        migration = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, expectedMemberUuid);
        out.writeObject(migration);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.MIGRATION_COMMIT;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", migration=").append(migration);
    }
}
