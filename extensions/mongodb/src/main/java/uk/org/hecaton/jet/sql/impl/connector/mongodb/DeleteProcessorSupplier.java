/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.org.hecaton.jet.sql.impl.connector.mongodb;

import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.mongodb.impl.UpdateMongoP;
import uk.org.hecaton.jet.mongodb.impl.WriteMongoP;
import uk.org.hecaton.jet.mongodb.impl.WriteMongoParams;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import static uk.org.hecaton.jet.mongodb.MongoSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static uk.org.hecaton.jet.mongodb.MongoSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static uk.org.hecaton.jet.mongodb.impl.MongoUtilities.UPDATE_ALL_PREDICATE;
import static uk.org.hecaton.jet.sql.impl.connector.mongodb.DynamicallyReplacedPlaceholder.replacePlaceholdersInPredicate;
import static java.util.Arrays.asList;

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance
 * that will delete given item.
 */
public class DeleteProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private String connectionString;
    private String databaseName;
    private String collectionName;
    private Serializable predicate;
    private String[] externalNames;
    private boolean hasInput;
    private String dataConnectionName;
    private String idField;
    private transient SupplierEx<MongoClient> clientSupplier;
    private ExpressionEvalContext evalContext;

    public DeleteProcessorSupplier() {
    }

    DeleteProcessorSupplier(MongoTable table, Serializable predicate, boolean hasInput) {
        this.connectionString = table.connectionString;
        this.databaseName = table.databaseName;
        this.dataConnectionName = table.dataConnectionName;
        this.collectionName = table.collectionName;
        this.externalNames = table.externalNames();
        this.idField = table.primaryKeyExternalName();
        this.predicate = predicate;
        this.hasInput = hasInput;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        if (connectionString != null) {
            clientSupplier = () -> MongoClients.create(connectionString);
        }
        evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        Processor[] processors = new Processor[count];

        for (int i = 0; i < count; i++) {
            Processor processor;
            if (hasInput) {
                processor = new WriteMongoP<>(
                        new WriteMongoParams<>()
                                .setClientSupplier(clientSupplier)
                                .setDataConnectionRef(dataConnectionName)
                                .setDatabaseName(databaseName)
                                .setCollectionName(collectionName)
                                .setDocumentType(Object.class)
                                .setCommitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY)
                                .setTransactionOptionsSup(() -> DEFAULT_TRANSACTION_OPTION)
                                .setIntermediateMappingFn(this::rowToDoc)
                                .setWriteModelFn(this::delete)
                );

            } else {
                Document predicateWithReplacements = predicate == null
                        ? UPDATE_ALL_PREDICATE
                        : replacePlaceholdersInPredicate(predicate, externalNames, evalContext);
                processor = new UpdateMongoP<>(
                        new WriteMongoParams<Document>()
                                .setClientSupplier(clientSupplier)
                                .setDataConnectionRef(dataConnectionName)
                                .setDatabaseName(databaseName)
                                .setCollectionName(collectionName)
                                .setDocumentType(Document.class),
                        () -> new DeleteManyModel<>(predicateWithReplacements)
                );

            }
            processors[i] = processor;
        }

        return asList(processors);
    }

    private WriteModel<Object> delete(Object pkValue) {
        return new DeleteManyModel<>(Filters.eq(idField, pkValue));
    }

    private Object rowToDoc(JetSqlRow row) {
        assert row.getFieldCount() == 1;

        return row.getValues()[0];
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(connectionString);
        out.writeString(databaseName);
        out.writeString(collectionName);
        out.writeObject(predicate);
        out.writeStringArray(externalNames);
        out.writeBoolean(hasInput);
        out.writeString(dataConnectionName);
        out.writeString(idField);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        connectionString = in.readString();
        databaseName = in.readString();
        collectionName = in.readString();
        predicate = in.readObject();
        externalNames = in.readStringArray();
        hasInput = in.readBoolean();
        dataConnectionName = in.readString();
        idField = in.readString();
    }
}
