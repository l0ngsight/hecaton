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

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.cluster.impl.operations.DemoteDataMemberOp;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.MigrationCycleOperation;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.PartitionDataSerializerHook;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;

/**
 * Request that the given member should no longer own any replica. The operation returns <code>true</code> if the
 * request was accepted and any needed migration has been scheduled.
 * After needed migrations have finished and the member no longer owns any replica, the member is notified with a
 * {@link DemoteResponseOperation}.
 *
 * @see DemoteDataMemberOp
 *
 * @since 5.4
 */
public class DemoteRequestOperation
        extends AbstractPartitionOperation implements MigrationCycleOperation {

    private UUID uuid;
    private transient boolean response;

    public DemoteRequestOperation() {
    }

    public DemoteRequestOperation(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();
        ILogger logger = getLogger();
        Address caller = getCallerAddress();

        if (partitionService.isLocalMemberMaster()) {
            ClusterService clusterService = getNodeEngine().getClusterService();
            Member member = clusterService.getMember(caller);
            if (member != null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Received demote request from %s", caller);
                }
                if (member.getUuid().equals(uuid)) {
                    response = partitionService.onDemoteRequest(member);
                } else {
                    logger.warning("Ignoring demote request from " + uuid + " because it is not a member");
                    response = false;
                }
            } else {
                logger.warning("Ignoring demote request from " + caller + " because it is not a member");
                response = false;
            }
        } else {
            logger.warning("Received demote request from " + caller + " but this node is not master.");
            response = false;
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getServiceName() {
        return IPartitionService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.DEMOTE_REQUEST;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        UUIDSerializationUtil.writeUUID(out, uuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        uuid = UUIDSerializationUtil.readUUID(in);
    }
}
