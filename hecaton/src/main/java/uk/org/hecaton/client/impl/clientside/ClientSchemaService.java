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

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ClientFetchSchemaCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientSendAllSchemasCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientSendSchemaCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.serialization.impl.compact.Schema;
import uk.org.hecaton.internal.serialization.impl.compact.SchemaService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.spi.properties.HecatonProperty;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static uk.org.hecaton.client.properties.ClientProperty.INVOCATION_RETRY_PAUSE_MILLIS;

public class ClientSchemaService implements SchemaService {

    public static final HecatonProperty MAX_PUT_RETRY_COUNT =
            new HecatonProperty("hecaton.client.schema.max.put.retry.count", 100);

    private final HecatonClientInstanceImpl client;
    private final Map<Long, Schema> schemas = new ConcurrentHashMap<>();
    private final ILogger logger;
    private final long retryPauseMillis;
    private final int maxPutRetryCount;

    public ClientSchemaService(HecatonClientInstanceImpl client, ILogger logger) {
        this.client = client;
        this.logger = logger;
        HecatonProperties properties = client.getProperties();
        retryPauseMillis = properties.getPositiveMillisOrDefault(INVOCATION_RETRY_PAUSE_MILLIS);
        maxPutRetryCount = properties.getInteger(MAX_PUT_RETRY_COUNT);
    }

    @Override
    public @Nullable Schema get(long schemaId) {
        Schema schema = schemas.get(schemaId);
        if (schema != null) {
            return schema;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Could not find schema id  %s locally, will search on the cluster%s", schemaId, schemaId);
        }
        ClientInvocation invocation = new ClientInvocation(client, ClientFetchSchemaCodec.encodeRequest(schemaId), SERVICE_NAME);
        ClientMessage message = invocation.invoke().joinInternal();
        schema = ClientFetchSchemaCodec.decodeResponse(message);
        if (schema != null) {
            schemas.putIfAbsent(schemaId, schema);
        }
        return schema;
    }

    @Override
    public void put(Schema schema) {
        long schemaId = schema.getSchemaId();
        Schema existingSchema = schemas.get(schemaId);
        if (existingSchema != null) {
            return;
        }

        if (!replicateSchemaInCluster(schema)) {
            throw new IllegalStateException("The schema " + schema + " cannot be "
                    + "replicated in the cluster, after " + maxPutRetryCount
                    + " retries. It might be the case that the client is "
                    + "connected to the two halves of the cluster that is "
                    + "experiencing a split-brain, and continue putting the "
                    + "data associated with that schema might result in data "
                    + "loss. It might be possible to replicate the schema "
                    + "after some time, when the cluster is healed.");
        }

        putIfAbsent(schema);
    }

    @Override
    public void putLocal(Schema schema) {
        putIfAbsent(schema);
    }

    private void putIfAbsent(Schema schema) {
        long schemaId = schema.getSchemaId();
        Schema existingSchema = schemas.putIfAbsent(schemaId, schema);
        if (existingSchema == null) {
            return;
        }

        if (!schema.equals(existingSchema)) {
            throw new IllegalStateException("Schema with schemaId " + schemaId + " already exists. "
                    + "existing schema " + existingSchema
                    + "new schema " + schema);
        }
    }

    public void sendAllSchemas() {
        if (schemas.isEmpty()) {
            if (logger.isFinestEnabled()) {
                logger.finest("There is no schema to send to the cluster");
            }
            return;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Sending schemas to the cluster %s", schemas);
        }
        ClientMessage clientMessage = ClientSendAllSchemasCodec.encodeRequest(new ArrayList<>(schemas.values()));
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, SERVICE_NAME);
        invocation.invokeUrgent().joinInternal();
    }

    public boolean hasAnySchemas() {
        return !schemas.isEmpty();
    }

    private boolean replicateSchemaInCluster(Schema schema) {
        ClientMessage clientMessage = ClientSendSchemaCodec.encodeRequest(schema);
        outer:
        for (int i = 0; i < maxPutRetryCount; i++) {
            ClientInvocation invocation = new ClientInvocation(client, clientMessage, SERVICE_NAME);
            ClientMessage response = invocation.invoke().joinInternal();
            Set<UUID> replicatedMemberUuids = ClientSendSchemaCodec.decodeResponse(response);
            Set<Member> members = client.getCluster().getMembers();
            for (Member member : members) {
                if (!replicatedMemberUuids.contains(member.getUuid())) {
                    // There is a member in our member list that the schema
                    // is not known to be replicated yet. We should retry
                    // sending it in a random member.

                    try {
                        Thread.sleep(retryPauseMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }

                    // correlation id will be set when the invoke method is
                    // called above
                    clientMessage = clientMessage.copyMessageWithSharedNonInitialFrames();

                    continue outer;
                }
            }

            // All members in our member list all known to have the schema
            return true;
        }

        // We tried to send it a couple of times, but the member list in our
        // local and the member list returned by the initiator nodes did not
        // match.
        return false;
    }
}
