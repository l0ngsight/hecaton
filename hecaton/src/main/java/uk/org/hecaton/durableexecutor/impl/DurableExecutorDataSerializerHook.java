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

package uk.org.hecaton.durableexecutor.impl;

import uk.org.hecaton.durableexecutor.impl.operations.DisposeResultBackupOperation;
import uk.org.hecaton.durableexecutor.impl.operations.DisposeResultOperation;
import uk.org.hecaton.durableexecutor.impl.operations.PutResultBackupOperation;
import uk.org.hecaton.durableexecutor.impl.operations.PutResultOperation;
import uk.org.hecaton.durableexecutor.impl.operations.ReplicationOperation;
import uk.org.hecaton.durableexecutor.impl.operations.RetrieveAndDisposeResultOperation;
import uk.org.hecaton.durableexecutor.impl.operations.RetrieveResultOperation;
import uk.org.hecaton.durableexecutor.impl.operations.ShutdownOperation;
import uk.org.hecaton.durableexecutor.impl.operations.TaskBackupOperation;
import uk.org.hecaton.durableexecutor.impl.operations.TaskOperation;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.DURABLE_EXECUTOR_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.DURABLE_EXECUTOR_DS_FACTORY_ID;

public class DurableExecutorDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(DURABLE_EXECUTOR_DS_FACTORY, DURABLE_EXECUTOR_DS_FACTORY_ID);

    public static final int DISPOSE_RESULT_BACKUP = 0;
    public static final int DISPOSE_RESULT = 1;
    public static final int PUT_RESULT = 2;
    public static final int REPLICATION = 3;
    public static final int RETRIEVE_DISPOSE_RESULT = 4;
    public static final int RETRIEVE_RESULT = 5;
    public static final int SHUTDOWN = 6;
    public static final int TASK_BACKUP = 7;
    public static final int TASK = 8;
    public static final int PUT_RESULT_BACKUP = 9;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> switch (typeId) {
            case DISPOSE_RESULT_BACKUP -> new DisposeResultBackupOperation();
            case DISPOSE_RESULT -> new DisposeResultOperation();
            case PUT_RESULT -> new PutResultOperation();
            case PUT_RESULT_BACKUP -> new PutResultBackupOperation();
            case REPLICATION -> new ReplicationOperation();
            case RETRIEVE_DISPOSE_RESULT -> new RetrieveAndDisposeResultOperation();
            case RETRIEVE_RESULT -> new RetrieveResultOperation();
            case SHUTDOWN -> new ShutdownOperation();
            case TASK_BACKUP -> new TaskBackupOperation();
            case TASK -> new TaskOperation();
            default -> null;
        };
    }
}
