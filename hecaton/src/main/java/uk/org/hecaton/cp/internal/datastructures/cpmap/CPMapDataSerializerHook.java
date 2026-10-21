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
package uk.org.hecaton.cp.internal.datastructures.cpmap;

import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.CompareAndSetOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.GetOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.PurgeOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.PutOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.RemoveOp;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public final class CPMapDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_CP_MAP_DS_FACTORY_ID = -1016;
    private static final String RAFT_CP_MAP_DS_FACTORY = "hecaton.serialization.ds.raft.cpmap";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_CP_MAP_DS_FACTORY, RAFT_CP_MAP_DS_FACTORY_ID);

    public static final int SNAPSHOT = 1;
    public static final int GET_OP = 2;
    public static final int PUT_OP = 3;
    public static final int REMOVE_OP = 4;
    public static final int COMPARE_AND_SET_OP = 5;
    public static final int PURGE_OP = 6;
    public static final int PURGE_RESPONSE = 7;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> switch (typeId) {
            case SNAPSHOT -> new CPMapSnapshot();
            case GET_OP -> new GetOp();
            case PUT_OP -> new PutOp();
            case REMOVE_OP -> new RemoveOp();
            case COMPARE_AND_SET_OP -> new CompareAndSetOp();
            case PURGE_OP -> new PurgeOp();
            case PURGE_RESPONSE -> new CPMapPurgeResponseImpl();
            default -> throw new IllegalArgumentException("Undefined type: " + typeId);
        };
    }
}
