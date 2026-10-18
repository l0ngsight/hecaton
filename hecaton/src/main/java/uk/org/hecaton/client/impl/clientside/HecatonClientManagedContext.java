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

package uk.org.hecaton.client.impl.clientside;

import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.core.ManagedContext;
import uk.org.hecaton.internal.serialization.SerializationServiceAware;

final class HecatonClientManagedContext implements ManagedContext {

    private final HecatonClientInstanceImpl instance;
    private final ManagedContext externalContext;
    private final boolean hasExternalContext;

    HecatonClientManagedContext(HecatonClientInstanceImpl instance, ManagedContext externalContext) {
        this.instance = instance;
        this.externalContext = externalContext;
        this.hasExternalContext = externalContext != null;
    }

    @Override
    public Object initialize(Object obj) {
        Object object = obj;
        if (object instanceof HecatonInstanceAware aware) {
            aware.setHecatonInstance(instance);
        }
        if (object instanceof SerializationServiceAware aware) {
            aware.setSerializationService(instance.getSerializationService());
        }

        if (hasExternalContext) {
            object = externalContext.initialize(object);
        }
        return object;
    }
}
