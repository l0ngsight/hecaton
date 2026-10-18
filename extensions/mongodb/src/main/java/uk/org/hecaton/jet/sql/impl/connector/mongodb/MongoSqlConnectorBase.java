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

import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.EventTimePolicy;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.mongodb.impl.DbCheckingPMetaSupplier;
import uk.org.hecaton.jet.mongodb.impl.DbCheckingPMetaSupplierBuilder;
import uk.org.hecaton.jet.sql.impl.connector.HecatonRexNode;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.schema.ConstantTableStatistics;
import uk.org.hecaton.sql.impl.schema.MappingField;
import uk.org.hecaton.sql.impl.schema.Table;
import uk.org.hecaton.sql.impl.schema.TableField;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.calcite.rex.RexNode;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static uk.org.hecaton.jet.mongodb.impl.Mappers.bsonToDocument;
import static uk.org.hecaton.jet.pipeline.DataConnectionRef.nullableDataConnectionRef;
import static uk.org.hecaton.sql.impl.QuoteIdentifierUtil.quoteCompoundIdentifier;
import static uk.org.hecaton.sql.impl.type.QueryDataType.OBJECT;
import static java.util.Collections.singletonList;

/**
 * Base for MongoDB SQL Connectors.
 * <p>
 * Streaming and batch connectors have similar way of dealing with scans with a few exceptions (like the requirement
 * for {@code _id field}).
 * <p>
 * All MongoDB connectors assume at least one primary key.
 * If user didn't specify any, the {@code _id} column is set to primary key - it is mandatory (auto-created if not
 * specified by user), unique and indexed.
 * <p>
 *
 * @see FieldResolver
 */
public abstract class MongoSqlConnectorBase implements SqlConnector {

    protected static final Set<String> ALLOWED_OBJECT_TYPES = Set.of("Collection", "ChangeStream");

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlExternalResource externalResource,
            @Nonnull List<MappingField> userFields) {
        if (externalResource.externalName().length > 2) {
            throw QueryException.error("Invalid external name " + quoteCompoundIdentifier(externalResource.externalName())
                    + ", external name for Mongo is allowed to have only one component (collection)"
                    + " or two components (database and collection)");
        }
        if (!ALLOWED_OBJECT_TYPES.contains(externalResource.objectType())) {
            throw QueryException.error("Mongo connector allows only object types: " + ALLOWED_OBJECT_TYPES);
        }
        FieldResolver fieldResolver = new FieldResolver(nodeEngine);
        return fieldResolver.resolveFields(externalResource.externalName(),
                externalResource.dataConnection(),
                externalResource.options(),
                userFields,
                isStream(externalResource.objectType()));
    }

    private static boolean isStream(String objectType) {
        return "ChangeStream".equalsIgnoreCase(objectType);
    }

    @Nonnull
    @Override
    public List<String> getPrimaryKey(Table table) {
        MongoTable mongoTable = (MongoTable) table;
        return singletonList(mongoTable.primaryKeyName());
    }

    @Nonnull
    @Override
    public String defaultObjectType() {
        return "Collection";
    }

    @Nonnull
    @Override
    public Table createTable(@Nonnull NodeEngine nodeEngine,
                             @Nonnull String schemaName, @Nonnull String mappingName,
                             @Nonnull SqlExternalResource externalResource,
                             @Nonnull List<MappingField> resolvedFields) {
        String objectType = checkNotNull(externalResource.objectType(),
                "object type is required in Mongo connector");
        if (!ALLOWED_OBJECT_TYPES.contains(objectType)) {
            throw QueryException.error("Mongo connector allows only object types: " + ALLOWED_OBJECT_TYPES);
        }
        String collectionName = externalResource.externalName().length == 2
                ? externalResource.externalName()[1]
                : externalResource.externalName()[0];
        FieldResolver fieldResolver = new FieldResolver(nodeEngine);
        String databaseName = Options.getDatabaseName(nodeEngine, externalResource.externalName(),
                externalResource.dataConnection());
        ConstantTableStatistics stats = new ConstantTableStatistics(0);

        List<TableField> fields = new ArrayList<>(resolvedFields.size());
        boolean containsId = false;
        boolean isStreaming = isStream(objectType);
        boolean hasPK = false;
        for (MappingField resolvedField : resolvedFields) {
            String externalNameFromName = (isStreaming ? "fullDocument." : "") + resolvedField.name();
            String fieldExternalName = firstNonNull(resolvedField.externalName(), externalNameFromName);
            String externalType = checkNotNull(resolvedField.externalType(),
                    "external type cannot be null in Mongo connector");

            if (fieldResolver.isId(fieldExternalName, isStreaming)) {
                containsId = true;
            }
            fields.add(new MongoTableField(
                    resolvedField.name(),
                    resolvedField.type(),
                    fieldExternalName,
                    false,
                    externalType,
                    resolvedField.isPrimaryKey()));
            hasPK |= resolvedField.isPrimaryKey();
        }

        if (!containsId) {
            if (isStreaming) {
                fields.add(0, new MongoTableField("fullDocument._id", OBJECT, "fullDocument._id",
                        true, "DOCUMENT", !hasPK));
            } else {
                fields.add(0, new MongoTableField("_id", OBJECT, "_id", true,
                        "DOCUMENT", !hasPK));
            }
        }
        return new MongoTable(schemaName, mappingName, databaseName, collectionName,
                externalResource.dataConnection(), externalResource.options(), this,
                fields, stats, objectType);
    }

    @Override
    @Nonnull
    public Vertex fullScanReader(
            @Nonnull DagBuildContext context,
            @Nullable HecatonRexNode predicate,
            @Nonnull List<HecatonRexNode> projection,
            @Nullable List<Map<String, Expression<?>>> partitionPruningCandidates,
            @Nullable FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        MongoTable table = context.getTable();

        RexToMongoVisitor visitor = new RexToMongoVisitor();

        Document filter = translateFilter(predicate, visitor);
        List<ProjectionData> projections = translateProjections(projection, context, visitor);

        DbCheckingPMetaSupplier supplier;
        final boolean forceReadTotalParallelismOne = table.isforceReadTotalParallelismOne();
        if (table.isStreaming()) {
            var startAt = Options.startAtTimestamp(table.getOptions());
            var ps = new SelectProcessorSupplier(table, filter, projections, startAt, eventTimePolicyProvider);
            supplier = wrap(context, ps, forceReadTotalParallelismOne);
        } else {
            var ps = new SelectProcessorSupplier(table, filter, projections);
            supplier = wrap(context, ps, forceReadTotalParallelismOne);
        }

        DAG dag = context.getDag();
        Vertex sourceVertex = dag.newUniqueVertex(
                "Select (" + table.getSqlName() + ")", supplier
        );
        if (forceReadTotalParallelismOne) {
            sourceVertex.localParallelism(1);
        }

        return sourceVertex;
    }

    protected static DbCheckingPMetaSupplier wrap(DagBuildContext ctx, ProcessorSupplier supplier) {
        return wrap(ctx, supplier, false);
    }

    protected static DbCheckingPMetaSupplier wrapWithParallelismOne(DagBuildContext ctx, ProcessorSupplier supplier) {
        return wrap(ctx, supplier, true);
    }

    protected static DbCheckingPMetaSupplier wrap(DagBuildContext ctx,
                                                  ProcessorSupplier supplier,
                                                  boolean forceParallelismOne) {
        MongoTable table = ctx.getTable();
        String connectionString = table.connectionString;
        SupplierEx<MongoClient> clientSupplier = connectionString == null
                ? null
                : () -> MongoClients.create(connectionString);
        return new DbCheckingPMetaSupplierBuilder()
                .withCheckResourceExistence(table.checkExistenceOnEachCall())
                .withForceTotalParallelismOne(table.isforceReadTotalParallelismOne())
                .withDatabaseName(table.databaseName)
                .withCollectionName(table.collectionName)
                .withClientSupplier(clientSupplier)
                .withDataConnectionRef(nullableDataConnectionRef(table.dataConnectionName))
                .withProcessorSupplier(supplier)
                .withForceTotalParallelismOne(forceParallelismOne)
                .build();
    }

    private static Document translateFilter(HecatonRexNode filterNode, RexToMongoVisitor visitor) {
        if (filterNode == null) {
            return null;
        }
        Object result = filterNode.unwrap(RexNode.class).accept(visitor);
        boolean isBson = result instanceof Bson;
        assert isBson || result instanceof InputRef;

        if (isBson) {
            return bsonToDocument((Bson) result);
        } else {
            InputRef placeholder = (InputRef) result;
            return bsonToDocument(Filters.eq(placeholder.asString(), true));
        }
    }

    private static List<ProjectionData> translateProjections(
            List<HecatonRexNode> projectionNodes,
            DagBuildContext context,
            RexToMongoVisitor visitor
    ) {
        List<ProjectionData> projection = new ArrayList<>();

        MongoTable table = context.getTable();
        String[] externalNames = table.externalNames();
        for (int i = 0; i < projectionNodes.size(); i++) {
            Object translated = projectionNodes.get(i).unwrap(RexNode.class).accept(visitor);
            InputRef ref = InputRef.match(translated);
            if (ref != null) {
                String externalName = externalNames[ref.getInputIndex()];
                Document projectionExpr = bsonToDocument(Projections.include(externalNames));
                projection.add(new ProjectionData(externalName, projectionExpr, i, table.fieldType(externalName)));
            } else {
                Document projectionExpr = new Document("projected_value_" + i, new Document("$literal", translated));
                projection.add(new ProjectionData("projected_value_" + i, projectionExpr, i, null));
            }
        }

        if (projection.isEmpty()) {
            throw new IllegalArgumentException("Projection list cannot be empty");
        }

        return projection;
    }

    @Override
    public boolean supportsExpression(@Nonnull HecatonRexNode expression) {
        RexToMongoVisitor visitor = new RexToMongoVisitor();
        RexNode rexNode = expression.unwrap(RexNode.class);
        try {
            rexNode.accept(visitor);
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }

    @Override
    public boolean dmlSupportsPredicates() {
        return true;
    }
}
