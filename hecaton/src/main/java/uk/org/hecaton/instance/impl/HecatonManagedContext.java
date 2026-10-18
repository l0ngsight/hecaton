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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.core.ManagedContext;
import uk.org.hecaton.internal.services.NodeAware;
import uk.org.hecaton.internal.serialization.SerializationServiceAware;

public final class HecatonManagedContext implements ManagedContext {

    private final HecatonInstanceImpl instance;
    private final ManagedContext externalContext;
    private final boolean hasExternalContext;

    public HecatonManagedContext(HecatonInstanceImpl instance, ManagedContext externalContext) {
        this.instance = instance;
        this.externalContext = externalContext;
        this.hasExternalContext = externalContext != null;
    }

    @Override
    public Object initialize(Object obj) {
        if (obj instanceof HecatonInstanceAware aware) {
            aware.setHecatonInstance(instance);
        }
        if (obj instanceof NodeAware aware) {
            aware.setNode(instance.node);
        }
        if (obj instanceof SerializationServiceAware aware) {
            aware.setSerializationService(instance.node.getSerializationService());
        }

        if (hasExternalContext) {
            return externalContext.initialize(obj);
        }
        return obj;
    }
}
