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

import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.internal.services.NodeAware;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.SpiDataSerializerHook;

import java.io.IOException;

/**
 * An {@link OperationFactory} that creates operations by cloning
 * the passed Operation.
 */
public final class BinaryOperationFactory implements OperationFactory, NodeAware, IdentifiedDataSerializable {

    private Data operationData;
    private NodeEngine nodeEngine;

    public BinaryOperationFactory() {
    }

    public BinaryOperationFactory(Operation operation, NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.operationData = nodeEngine.toData(operation);
    }

    public BinaryOperationFactory(Data operationData) {
        this.operationData = operationData;
    }

    @Override
    public Operation createOperation() {
        return nodeEngine.toObject(operationData);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, operationData);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operationData = IOUtil.readData(in);
    }

    @Override
    public void setNode(Node node) {
        this.nodeEngine = node.nodeEngine;
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.PARALLEL_OPERATION_FACTORY;
    }
}
