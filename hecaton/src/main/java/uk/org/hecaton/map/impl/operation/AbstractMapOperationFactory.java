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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.util.concurrent.Callable;

public abstract class AbstractMapOperationFactory implements OperationFactory {

    protected String name;

    protected AbstractMapOperationFactory() {
    }

    protected AbstractMapOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public final int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public String getName() {
        return name;
    }

    /**
     * Utility method for executing code within the context of the Namespace associated
     * with IMaps - if one does not exist, this code is executed as if it were called
     * directly in place of this method.
     */
    protected <T> T callWithNamespaceAwareness(Callable<T> callable) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        String namespace = MapService.lookupNamespace(engine, name);
        return NamespaceUtil.callWithNamespace(engine, namespace, callable);
    }
}
