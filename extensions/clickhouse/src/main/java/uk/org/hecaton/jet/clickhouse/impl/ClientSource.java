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

package uk.org.hecaton.jet.clickhouse.impl;

import uk.org.hecaton.dataconnection.DataConnectionService;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.clickhouse.ClickHouseDataConnection;
import uk.org.hecaton.jet.pipeline.DataConnectionRef;
import com.clickhouse.client.api.Client;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

import static uk.org.hecaton.internal.util.Preconditions.checkState;

/**
 * Where a ClickHouse client comes from: a {@link ClickHouseDataConnection}, or a function that
 * creates one.
 */
public final class ClientSource implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataConnectionRef dataConnectionRef;
    private final SupplierEx<Client> clientFn;

    private ClientSource(DataConnectionRef dataConnectionRef, SupplierEx<Client> clientFn) {
        this.dataConnectionRef = dataConnectionRef;
        this.clientFn = clientFn;
    }

    /**
     * Clients of the named data connection.
     */
    public static ClientSource of(@Nonnull DataConnectionRef dataConnectionRef) {
        return new ClientSource(dataConnectionRef, null);
    }

    /**
     * Clients created by the function; each is closed after use.
     */
    public static ClientSource of(@Nonnull SupplierEx<Client> clientFn) {
        return new ClientSource(null, clientFn);
    }

    /**
     * Returns a client, to be closed with the returned lease.
     *
     * @param dataConnectionService the member's data connections, needed if the client comes
     *                              from a data connection
     */
    public Lease open(@Nullable DataConnectionService dataConnectionService) {
        if (clientFn != null) {
            Client client = clientFn.get();
            return new Lease(client, client::close);
        }
        checkState(dataConnectionService != null, "No data connection service to look up '"
                + dataConnectionRef.getName() + "'");
        ClickHouseDataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(
                dataConnectionRef.getName(), ClickHouseDataConnection.class);
        try {
            Client client = dataConnection.getClient();
            boolean shared = dataConnection.getConfig().isShared();
            return new Lease(client, () -> {
                try {
                    if (!shared) {
                        client.close();
                    }
                } finally {
                    dataConnection.release();
                }
            });
        } catch (RuntimeException e) {
            dataConnection.release();
            throw e;
        }
    }

    @Override
    public String toString() {
        return dataConnectionRef != null ? "data connection '" + dataConnectionRef.getName() + "'" : "client function";
    }

    /**
     * A client in use: closing the lease closes the client or returns it to its data connection.
     */
    public static final class Lease implements AutoCloseable {

        private final Client client;
        private final Runnable onClose;

        Lease(Client client, Runnable onClose) {
            this.client = client;
            this.onClose = onClose;
        }

        public Client client() {
            return client;
        }

        @Override
        public void close() {
            onClose.run();
        }
    }
}
