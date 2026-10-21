/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.cp.internal.datastructures.cpmap.operation;

import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapDataSerializerHook;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

/**
 * Operation for {@link uk.org.hecaton.cp.CPMap#get(Object)}.
 */
public class GetOp extends AbstractCPMapOp implements ReadonlyOperation {

    public GetOp() {
    }

    public GetOp(String name, Data key) {
        super(name, key);
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        return getContainer(groupId).get(key);
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.GET_OP;
    }
}
