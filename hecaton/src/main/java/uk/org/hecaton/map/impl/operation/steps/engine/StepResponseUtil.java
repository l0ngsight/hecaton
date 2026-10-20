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

package uk.org.hecaton.map.impl.operation.steps.engine;

import uk.org.hecaton.instance.impl.OutOfMemoryErrorDispatcher;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.operation.MapOperation;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationexecutor.OperationRunner;
import uk.org.hecaton.spi.impl.operationservice.Notifier;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationRunnerImpl;

import static uk.org.hecaton.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static uk.org.hecaton.map.impl.record.Record.UNSET;

/**
 * Util for sending response after executing {@link Step}.
 */
public final class StepResponseUtil {

    private StepResponseUtil() {
    }

    /**
     * This method:
     * <lu>
     * <li>Applies final state to operation to extract response</li>
     * <li>Sends response to caller</li>
     * <li>Sends backup operation if there is backup</li>
     * <li>Notifies parked operations</li>
     * </lu>
     */
    public static void sendResponse(State state) {
        assert isRunningOnPartitionThread();

        MapOperation operation = state.getOperation();
        operation.applyState(state);

        handleBackupAndSendResponse(state);

        try {
            if (operation instanceof Notifier notifier) {
                if (notifier.shouldNotify()) {
                    getNodeEngine(state).getOperationParker().unpark(notifier);
                }
            }
        } catch (Throwable e) {
            logOperationError(operation, e);
        }
    }

    private static NodeEngineImpl getNodeEngine(State state) {
        return (NodeEngineImpl) state.getOperation().getNodeEngine();
    }

    private static void logOperationError(Operation op, Throwable e) {
        if (e instanceof OutOfMemoryError error) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(error);
        }
        op.logError(e);
    }

    private static void handleBackupAndSendResponse(State state) {
        assert state.getPartitionId() != UNSET;

        OperationRunner runner = getOperationService(state)
                .getOperationExecutor().getPartitionOperationRunners()[state.getPartitionId()];

        ((OperationRunnerImpl) runner).sendBackupsAndResponse(state.getOperation());
    }

    private static OperationService getOperationService(State state) {
        MapContainer mapContainer = state.getOperation().getMapContainer();
        MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        return mapServiceContext.getNodeEngine().getOperationService();
    }
}
