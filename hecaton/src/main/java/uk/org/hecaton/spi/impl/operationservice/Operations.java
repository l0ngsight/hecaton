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

package uk.org.hecaton.spi.impl.operationservice;

import uk.org.hecaton.internal.cluster.impl.operations.JoinOperation;
import uk.org.hecaton.internal.cluster.impl.operations.WanOperation;
import uk.org.hecaton.internal.partition.MigrationCycleOperation;

/**
 * Utility class that contains helper methods related to {@link Operation}
 */
public final class Operations {

    private static final ClassLoader THIS_CLASS_LOADER = OperationAccessor.class.getClassLoader();

    private Operations() {
    }

    public static boolean isJoinOperation(Operation op) {
        return op instanceof JoinOperation
                && op.getClass().getClassLoader() == THIS_CLASS_LOADER;
    }

    public static boolean isMigrationOperation(Operation op) {
        return op instanceof MigrationCycleOperation
                && op.getClass().getClassLoader() == THIS_CLASS_LOADER;
    }

    /**
     *  Checks if the given operation is an instance of {@link WanOperation}
     */
    public static boolean isWanReplicationOperation(Operation op) {
        return op instanceof WanOperation
                && op.getClass().getClassLoader() == THIS_CLASS_LOADER;
    }
}
