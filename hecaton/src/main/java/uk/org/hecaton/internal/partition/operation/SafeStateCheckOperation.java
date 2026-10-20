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

import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.impl.PartitionDataSerializerHook;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;

/**
 * Checks whether a node is safe or not.
 * Safe means, first backups of partitions those owned by local member are sync with primary.
 *
 * @see PartitionService#isClusterSafe
 * @see PartitionService#isMemberSafe
 */
public class SafeStateCheckOperation extends AbstractPartitionOperation implements AllowedDuringPassiveState {

    private transient boolean safe;
    private transient boolean throwWhenTargetNotMember;

    public SafeStateCheckOperation() {
    }

    public SafeStateCheckOperation(boolean throwWhenTargetNotMember) {
        this.throwWhenTargetNotMember = throwWhenTargetNotMember;
    }

    @Override
    public void run() throws Exception {
        final InternalPartitionService service = getService();
        safe = service.isMemberStateSafe();
    }

    @Override
    public Object getResponse() {
        return safe;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.SAFE_STATE_CHECK;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return throwable instanceof TargetNotMemberException && throwWhenTargetNotMember
                ? ExceptionAction.THROW_EXCEPTION : super.onInvocationException(throwable);
    }
}
