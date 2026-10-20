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

package uk.org.hecaton.jet.impl.processor;

import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.impl.execution.init.JetInitDataSerializerHook;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.PermissionsUtil;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serial;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProcessorSupplierFromSimpleSupplier implements ProcessorSupplier, IdentifiedDataSerializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private SupplierEx<? extends Processor> simpleSupplier;

    // for deserialization
    @SuppressWarnings("unused")
    public ProcessorSupplierFromSimpleSupplier() {
    }

    public ProcessorSupplierFromSimpleSupplier(SupplierEx<? extends Processor> simpleSupplier) {
        this.simpleSupplier = simpleSupplier;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        PermissionsUtil.checkPermission(simpleSupplier, context);
    }

    @Override
    public boolean initIsCooperative() {
        return true;
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }

    @Nonnull @Override
    public Collection<? extends Processor> get(int count) {
        return Stream.generate(simpleSupplier).limit(count).collect(Collectors.toList());
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.PROCESSOR_SUPPLIER_FROM_SIMPLE_SUPPLIER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(simpleSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        simpleSupplier = in.readObject();
    }
}
