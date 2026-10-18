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

package uk.org.hecaton.internal.crdt;

import uk.org.hecaton.internal.crdt.pncounter.PNCounterImpl;
import uk.org.hecaton.internal.crdt.pncounter.PNCounterReplicationOperation;
import uk.org.hecaton.internal.crdt.pncounter.operations.AddOperation;
import uk.org.hecaton.internal.crdt.pncounter.operations.CRDTTimestampedLong;
import uk.org.hecaton.internal.crdt.pncounter.operations.GetOperation;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.PN_COUNTER_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.PN_COUNTER_DS_FACTORY_ID;

/**
 * Serialization constants for all CRDT related objects
 */
public final class CRDTDataSerializerHook implements DataSerializerHook {
    /** The CRDT (de)serializer factory ID */
    public static final int F_ID = FactoryIdHelper.getFactoryId(PN_COUNTER_DS_FACTORY, PN_COUNTER_DS_FACTORY_ID);
    /** The (de)serialization constant for a PNCounter replication operation */
    public static final int PN_COUNTER_REPLICATION = 1;
    /** The (de)serialization constant for a PNCounter implementation */
    public static final int PN_COUNTER = 2;
    /** The constant for the PNCounter {@link AddOperation} */
    public static final int PN_COUNTER_ADD_OPERATION = 3;
    /** The constant for the PNCounter {@link GetOperation} */
    public static final int PN_COUNTER_GET_OPERATION = 4;
    /** The constant for the PNCounter {@link CRDTTimestampedLong} */
    public static final int CRDT_TIMESTAMPED_LONG = 5;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> switch (typeId) {
            case PN_COUNTER_REPLICATION -> new PNCounterReplicationOperation();
            case PN_COUNTER -> new PNCounterImpl();
            case PN_COUNTER_ADD_OPERATION -> new AddOperation();
            case PN_COUNTER_GET_OPERATION -> new GetOperation();
            case CRDT_TIMESTAMPED_LONG -> new CRDTTimestampedLong();
            default -> null;
        };
    }
}
