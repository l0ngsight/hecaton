/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal.datastructures.atomicref;

import uk.org.hecaton.cp.internal.datastructures.atomicref.operation.ApplyOp;
import uk.org.hecaton.cp.internal.datastructures.atomicref.operation.CompareAndSetOp;
import uk.org.hecaton.cp.internal.datastructures.atomicref.operation.ContainsOp;
import uk.org.hecaton.cp.internal.datastructures.atomicref.operation.GetOp;
import uk.org.hecaton.cp.internal.datastructures.atomicref.operation.SetOp;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public final class AtomicRefDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_ATOMIC_REF_DS_FACTORY_ID = -1014;
    private static final String RAFT_ATOMIC_REF_DS_FACTORY = "hecaton.serialization.ds.raft.atomicref";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_ATOMIC_REF_DS_FACTORY, RAFT_ATOMIC_REF_DS_FACTORY_ID);

    public static final int SNAPSHOT = 1;
    public static final int APPLY_OP = 2;
    public static final int COMPARE_AND_SET_OP = 3;
    public static final int CONTAINS_OP = 4;
    public static final int GET_OP = 5;
    public static final int SET_OP = 6;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case SNAPSHOT:
                    return new AtomicRefSnapshot();
                case APPLY_OP:
                    return new ApplyOp();
                case COMPARE_AND_SET_OP:
                    return new CompareAndSetOp();
                case CONTAINS_OP:
                    return new ContainsOp();
                case GET_OP:
                    return new GetOp();
                case SET_OP:
                    return new SetOp();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
