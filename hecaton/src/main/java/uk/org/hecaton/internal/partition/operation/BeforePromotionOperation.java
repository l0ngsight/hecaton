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

import uk.org.hecaton.internal.partition.MigrationInfo;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.PartitionStateManager;
import uk.org.hecaton.internal.partition.operation.PromotionCommitOperation.PromotionOperationCallback;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.partition.ReplicaMigrationEvent;
import uk.org.hecaton.internal.partition.MigrationAwareService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;

/**
 * Runs locally when the node becomes owner of a partition, before applying a promotion result to the partition table.
 * Sends a {@link ReplicaMigrationEvent} and notifies all {@link MigrationAwareService}s that the migration is starting.
 * After completion notifies the {@link #beforePromotionsCallback}.
 */
final class BeforePromotionOperation extends AbstractPromotionOperation {

    private PromotionOperationCallback beforePromotionsCallback;

    /**
     * This constructor should not be used to obtain an instance of this class; it exists to fulfill IdentifiedDataSerializable
     * coding conventions.
     */
    BeforePromotionOperation() {
        super(null);
    }

    BeforePromotionOperation(MigrationInfo migrationInfo, PromotionOperationCallback callback) {
        super(migrationInfo);
        this.beforePromotionsCallback = callback;
    }

    @Override
    public void beforeRun() {
        InternalPartitionServiceImpl service = getService();
        PartitionStateManager partitionStateManager = service.getPartitionStateManager();
        if (!partitionStateManager.trySetMigratingFlag(getPartitionId())) {
            throw new IllegalStateException("Cannot set migrating flag, "
                    + "probably previous migration's finalization is not completed yet.");
        }
    }

    @Override
    public void run() {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Running before promotion for %s", getPartitionMigrationEvent());
        }

        PartitionMigrationEvent event = getPartitionMigrationEvent();
        for (MigrationAwareService service : getMigrationAwareServices()) {
            try {
                service.beforeMigration(event);
            } catch (Throwable e) {
                logger.warning("While promoting " + getPartitionMigrationEvent(), e);
            }
        }
    }

    @Override
    public void afterRun() {
        if (beforePromotionsCallback != null) {
            beforePromotionsCallback.onComplete(migrationInfo);
        }
    }
}
