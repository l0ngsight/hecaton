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

package uk.org.hecaton.jet.clickhouse;

import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.dataconnection.DataConnectionBase;
import uk.org.hecaton.dataconnection.DataConnectionResource;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static uk.org.hecaton.internal.util.Preconditions.checkState;

/**
 * A data connection to a ClickHouse server or cluster, used by {@link ClickHouseMapStore},
 * {@link ClickHouseSources} and {@link ClickHouseSinks}. It connects with the HTTP interface,
 * through the ClickHouse Java client.
 * <p>
 * Properties:
 * <ul>
 *     <li>{@code endpoints} (required): the HTTP(S) URLs of the servers, separated by commas,
 *     for example {@code http://clickhouse-1:8123,http://clickhouse-2:8123}. The client sends each
 *     request to one of them;</li>
 *     <li>{@code username} (default {@code default}) and {@code password} (default empty);</li>
 *     <li>{@code database}: the default database, for tables named without a database;</li>
 *     <li>{@code client.<option>}: any option of the ClickHouse Java client, for example
 *     {@code client.socket_timeout} or {@code client.compress}.</li>
 * </ul>
 * A shared data connection (the default) has one client, which all users share. A data
 * connection that is not shared creates a client for each user.
 *
 * @since 1.0
 */
public class ClickHouseDataConnection extends DataConnectionBase {

    /**
     * The type of the data connection in configuration: {@value}.
     */
    public static final String TYPE = "ClickHouse";
    /**
     * The HTTP(S) URLs of the servers, separated by commas.
     */
    public static final String ENDPOINTS_PROPERTY = "endpoints";
    /**
     * The user name.
     */
    public static final String USERNAME_PROPERTY = "username";
    /**
     * The password.
     */
    public static final String PASSWORD_PROPERTY = "password";
    /**
     * The default database.
     */
    public static final String DATABASE_PROPERTY = "database";
    /**
     * The prefix of the properties passed to the ClickHouse client as options.
     */
    public static final String CLIENT_OPTION_PREFIX = "client.";

    private volatile Client sharedClient;

    /**
     * Creates the data connection from its configuration.
     */
    public ClickHouseDataConnection(@Nonnull DataConnectionConfig config) {
        super(config);
        String endpoints = config.getProperty(ENDPOINTS_PROPERTY);
        checkState(endpoints != null && !endpoints.isBlank(),
                "The ClickHouse data connection '" + config.getName() + "' needs the property '"
                        + ENDPOINTS_PROPERTY + "'");
        if (config.isShared()) {
            sharedClient = createClient(config.getName(), config.getProperties());
        }
    }

    /**
     * Creates a ClickHouse client from the properties of a data connection.
     */
    static Client createClient(String name, Properties properties) {
        try {
            Client.Builder builder = new Client.Builder();
            for (String endpoint : properties.getProperty(ENDPOINTS_PROPERTY, "").split(",")) {
                if (!endpoint.isBlank()) {
                    builder.addEndpoint(endpoint.strip());
                }
            }
            builder.setUsername(properties.getProperty(USERNAME_PROPERTY, "default"))
                   .setPassword(properties.getProperty(PASSWORD_PROPERTY, ""))
                   .setClientName("hecaton");
            String database = properties.getProperty(DATABASE_PROPERTY);
            if (database != null) {
                builder.setDefaultDatabase(database);
            }
            for (String key : properties.stringPropertyNames()) {
                if (key.startsWith(CLIENT_OPTION_PREFIX)) {
                    builder.setOption(key.substring(CLIENT_OPTION_PREFIX.length()), properties.getProperty(key));
                }
            }
            return builder.build();
        } catch (Exception e) {
            throw new HecatonException("Unable to create the ClickHouse client of '" + name + "': " + e.getMessage(), e);
        }
    }

    /**
     * Returns a ClickHouse client. If the data connection is shared, the client is the shared
     * one: do not close it, the data connection closes it when it is removed. Otherwise the
     * client is new, and the caller closes it.
     */
    @Nonnull
    public Client getClient() {
        if (getConfig().isShared()) {
            Client client = sharedClient;
            checkState(client != null, "The ClickHouse data connection '" + getName() + "' is closed");
            return client;
        }
        return createClient(getName(), getConfig().getProperties());
    }

    /**
     * Returns the tables of the default database, or of all databases but the system ones if
     * the data connection has no default database.
     */
    @Nonnull
    @Override
    public List<DataConnectionResource> listResources() {
        String sql = getConfig().getProperty(DATABASE_PROPERTY) != null
                ? "SELECT database, name FROM system.tables WHERE database = currentDatabase() ORDER BY name"
                : "SELECT database, name FROM system.tables WHERE database NOT IN "
                        + "('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY database, name";
        Client client = getClient();
        try (QueryResponse response = client.query(sql).get();
             ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
            List<DataConnectionResource> resources = new ArrayList<>();
            while (reader.hasNext()) {
                reader.next();
                resources.add(new DataConnectionResource("Table", reader.getString(1), reader.getString(2)));
            }
            return resources;
        } catch (Exception e) {
            throw new HecatonException("Unable to list the tables of the ClickHouse data connection '" + getName()
                    + "': " + e.getMessage(), e);
        } finally {
            if (!getConfig().isShared()) {
                client.close();
            }
        }
    }

    @Nonnull
    @Override
    public Collection<String> resourceTypes() {
        return singletonList("Table");
    }

    /**
     * Closes the shared client.
     */
    @Override
    public void destroy() {
        Client client = sharedClient;
        sharedClient = null;
        if (client != null) {
            client.close();
        }
    }
}
