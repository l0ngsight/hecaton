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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.security.PermissionsUtil;

import javax.annotation.Nonnull;
import java.io.Serial;

/**
 * A ProcessorSupplier that checks permission of processor function
 */
class ProcessorFunctionConnectorSupplier extends AbstractHecatonConnectorSupplier {

    @Serial
    private static final long serialVersionUID = 1L;

    private final FunctionEx<HecatonInstance, Processor> processorFunction;

    ProcessorFunctionConnectorSupplier(
            String dataConnectionName,
            String clientXml,
            FunctionEx<HecatonInstance, Processor> processorFunction
    ) {
        super(dataConnectionName, clientXml);
        this.processorFunction = processorFunction;
    }

    @Override
    public void init(@Nonnull Context context) {
        PermissionsUtil.checkPermission(processorFunction, context);
        super.init(context);
    }

    @Override
    protected Processor createProcessor(HecatonInstance instance, SerializationService serializationService) {
        return processorFunction.apply(instance);
    }
}
