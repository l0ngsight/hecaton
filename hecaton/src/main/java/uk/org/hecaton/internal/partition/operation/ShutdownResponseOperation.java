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
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.MigrationCycleOperation;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.PartitionDataSerializerHook;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.impl.Versioned;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.io.IOException;
import java.util.UUID;

public class ShutdownResponseOperation
        extends AbstractPartitionOperation implements MigrationCycleOperation, Versioned {

    private UUID uuid;

    public ShutdownResponseOperation() {
    }

    public ShutdownResponseOperation(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();
        ILogger logger = getLogger();
        Address caller = getCallerAddress();

        NodeEngine nodeEngine = getNodeEngine();
        if (nodeEngine.isRunning()) {
            logger.severe("Received a shutdown response from " + caller + ", but this node is not shutting down!");
            return;
        }

        if (partitionService.isMemberMaster(caller)) {
            if (logger.isFinestEnabled()) {
                logger.finest("Received shutdown response from %s", caller);
            }

            if (nodeEngine.getLocalMember().getUuid().equals(uuid)) {
                partitionService.onShutdownResponse();
            } else {
                logger.warning("Ignoring shutdown response for " + uuid + " since it's not the expected member");
            }
        } else {
            logger.warning("Received shutdown response from " + caller + " but it's not the known master");
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return IPartitionService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.SHUTDOWN_RESPONSE;
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
