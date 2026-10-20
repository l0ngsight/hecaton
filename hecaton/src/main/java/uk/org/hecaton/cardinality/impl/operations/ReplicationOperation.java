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

package uk.org.hecaton.cardinality.impl.operations;

import uk.org.hecaton.cardinality.impl.CardinalityEstimatorContainer;
import uk.org.hecaton.cardinality.impl.CardinalityEstimatorDataSerializerHook;
import uk.org.hecaton.cardinality.impl.CardinalityEstimatorService;
import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;

public class ReplicationOperation
        extends Operation
        implements IdentifiedDataSerializable {

    private Map<String, CardinalityEstimatorContainer> migrationData;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<String, CardinalityEstimatorContainer> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        CardinalityEstimatorService service = getService();
        for (Map.Entry<String, CardinalityEstimatorContainer> entry : migrationData.entrySet()) {
            String name = entry.getKey();
            service.addCardinalityEstimator(name, entry.getValue());
        }
    }

    @Override
    public String getServiceName() {
        return CardinalityEstimatorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CardinalityEstimatorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CardinalityEstimatorDataSerializerHook.REPLICATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeMapStringKey(migrationData, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        migrationData = SerializationUtil.readMapStringKey(in);
    }
}
