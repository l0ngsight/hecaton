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

import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.dataconnection.HecatonDataConnection;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import uk.org.hecaton.security.PermissionsUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serial;
import java.util.Collection;
import java.util.stream.Stream;

import static uk.org.hecaton.client.HecatonClient.newHecatonClient;
import static uk.org.hecaton.jet.impl.util.ImdgUtil.asClientConfig;
import static java.util.stream.Collectors.toList;

public abstract class AbstractHecatonConnectorSupplier implements ProcessorSupplier {

    protected final String dataConnectionName;
    protected final String clientXml;

    private transient HecatonInstance instance;
    private transient SerializationService serializationService;

    protected AbstractHecatonConnectorSupplier(
            @Nullable String dataConnectionName,
            @Nullable String clientXml
    ) {
        this.clientXml = clientXml;
        this.dataConnectionName = dataConnectionName;
    }

    public static ProcessorSupplier ofMap(
            @Nullable String clientXml,
            @Nonnull FunctionEx<HecatonInstance, Processor> procFn
    ) {
        return new AbstractHecatonConnectorSupplier(null, clientXml) {
            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            public void init(@Nonnull Context context) {
                PermissionsUtil.checkPermission(procFn, context);
                super.init(context);
            }

            @Override
            protected Processor createProcessor(HecatonInstance instance, SerializationService serializationService) {
                return procFn.apply(instance);
            }
        };
    }

    @Override
    public void init(@Nonnull Context context) {
        createHzClient(context);
    }

    private void createHzClient(ProcessorSupplier.Context context) {
        // The order is important.
        // If dataConnectionConfig is specified prefer it to clientXml
        if (dataConnectionName != null) {
            HecatonDataConnection hecatonDataConnection = context
                    .dataConnectionService()
                    .getAndRetainDataConnection(dataConnectionName, HecatonDataConnection.class);
            try {
                instance =  hecatonDataConnection.getClient();
                serializationService = ((HecatonClientProxy) instance).getSerializationService();
            } finally {
                hecatonDataConnection.release();
            }
        } else if (clientXml != null) {
            instance = newHecatonClient(asClientConfig(clientXml));
            serializationService = ((HecatonClientProxy) instance).getSerializationService();
        } else {
            instance = context.hecatonInstance();
            serializationService = ((ProcSupplierCtx) context).serializationService();
        }
    }

    @Nonnull @Override
    public Collection<? extends Processor> get(int count) {
        return Stream.generate(() -> createProcessor(instance, serializationService))
                     .limit(count)
                     .collect(toList());
    }

    protected abstract Processor createProcessor(HecatonInstance instance, SerializationService serializationService);

    /**
     * Return if ProcessorSupplier is for local cluster or not
     */
    boolean isLocal() {
        return (clientXml == null) && (dataConnectionName == null);
    }

    /**
     * Return if ProcessorSupplier is for remote cluster or not
     */
    boolean isRemote() {
        return !isLocal();
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (!isLocal() && instance != null) {
            instance.shutdown();
        }
    }
}
