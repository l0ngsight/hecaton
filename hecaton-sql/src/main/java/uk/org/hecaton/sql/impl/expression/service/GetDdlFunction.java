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

package uk.org.hecaton.sql.impl.expression.service;

import uk.org.hecaton.jet.impl.execution.CooperativeThread;
import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.security.permission.SqlPermission;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.QueryUtils;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.expression.TriExpression;
import uk.org.hecaton.sql.impl.row.Row;
import uk.org.hecaton.sql.impl.schema.Mapping;
import uk.org.hecaton.sql.impl.schema.SqlCatalogObject;
import uk.org.hecaton.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import static uk.org.hecaton.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_NAME;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_VIEW_DATACONNECTION;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_VIEW_MAPPING;
import static uk.org.hecaton.sql.impl.expression.string.StringFunctionUtils.asVarchar;

public class GetDdlFunction extends TriExpression<String> {
    static final String RELATION_NAMESPACE = "relation";
    static final String DATACONNECTION_NAMESPACE = "dataconnection";

    public GetDdlFunction() {
    }

    public GetDdlFunction(Expression<?> op1, Expression<?> op2, Expression<?> op3) {
        super(op1, op2, op3);
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        CooperativeThread.checkNonCooperative();

        String namespace = asVarchar(operand1, row, context);
        if (namespace == null) {
            throw QueryException.error("Namespace must not be null for GET_DDL");
        }

        String objectName = asVarchar(operand2, row, context);
        if (objectName == null) {
            throw QueryException.error("Object_name must not be null for GET_DDL");
        }

        // Ignore schema for now, the only supported schema at the moment is 'hecaton.public'.

        IMap<?, ?> sqlCatalog = context.getNodeEngine().getHecatonInstance().getMap(SQL_CATALOG_MAP_NAME);
        final String ddl;
        if (!(namespace.equals(RELATION_NAMESPACE) || namespace.equals(DATACONNECTION_NAMESPACE))) {
            throw QueryException.error(
                    "Namespace '" + namespace + "' is not supported. Only '" + RELATION_NAMESPACE + "' and '"
                            + DATACONNECTION_NAMESPACE + "' namespaces are supported.");
        }

        String keyName = objectName;
        if (namespace.equals(DATACONNECTION_NAMESPACE)) {
            keyName = QueryUtils.wrapDataConnectionKey(objectName);
        }

        final Object obj = sqlCatalog.get(keyName);
        if (obj == null) {
            throw QueryException.error("Object '" + objectName + "' does not exist in namespace '" + namespace + "'");
        } else if (obj instanceof SqlCatalogObject catalogObject) {
            if (catalogObject instanceof Mapping) {
                context.checkPermission(new SqlPermission(catalogObject.name(), ACTION_VIEW_MAPPING));
            } else if (catalogObject instanceof DataConnectionCatalogEntry) {
                context.checkPermission(new SqlPermission(catalogObject.name(), ACTION_VIEW_DATACONNECTION));
            }
            // Note: both view and type can't contain sensitive information -> we don't check them
            ddl = catalogObject.unparse();
        } else {
            throw new AssertionError("Object must not be present in information_schema");
        }
        return ddl;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_GET_DDL;
    }

    public static GetDdlFunction create(
            Expression<?> namespace,
            Expression<?> objectName,
            Expression<?> schema) {
        return new GetDdlFunction(namespace, objectName, schema);
    }
}
