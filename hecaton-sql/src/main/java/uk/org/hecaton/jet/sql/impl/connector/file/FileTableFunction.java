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

package uk.org.hecaton.jet.sql.impl.connector.file;

import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.jet.sql.impl.schema.HecatonDynamicTableFunction;
import uk.org.hecaton.jet.sql.impl.schema.HecatonSqlOperandMetadata;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTableFunctionParameter;
import uk.org.hecaton.jet.sql.impl.validate.HecatonCallBinding;
import uk.org.hecaton.jet.sql.impl.validate.HecatonSqlValidator;
import uk.org.hecaton.jet.sql.impl.validate.operand.TypedOperandChecker;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.HecatonOperandTypeInference;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ConnectorPermission;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.schema.MappingField;
import uk.org.hecaton.sql.impl.schema.Table;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;

import java.security.Permission;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static uk.org.hecaton.jet.sql.impl.connector.file.FileSqlConnector.OPTION_OPTIONS;
import static uk.org.hecaton.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static uk.org.hecaton.jet.sql.impl.connector.file.FileSqlConnector.OPTION_SHARED_FILE_SYSTEM;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.MAP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class FileTableFunction extends HecatonDynamicTableFunction {

    private static final String SCHEMA_NAME_FILES = "files";
    private static final List<HecatonTableFunctionParameter> PARAMETERS = asList(
            new HecatonTableFunctionParameter(0, OPTION_PATH, VARCHAR, false, TypedOperandChecker.VARCHAR),
            new HecatonTableFunctionParameter(1, OPTION_GLOB, VARCHAR, true, TypedOperandChecker.VARCHAR),
            new HecatonTableFunctionParameter(2, OPTION_SHARED_FILE_SYSTEM, VARCHAR, true, TypedOperandChecker.VARCHAR),
            new HecatonTableFunctionParameter(3, OPTION_OPTIONS, MAP, true, TypedOperandChecker.MAP)
    );

    public FileTableFunction(String name, String format) {
        super(name, FileOperandMetadata.INSTANCE, arguments -> toTable(arguments, format));
    }

    private static Table toTable(List<Object> arguments, String format) {
        Map<String, Object> options = toOptions(arguments, format);
        List<MappingField> fields = FileSqlConnector.resolveAndValidateFields(options, emptyList());
        return FileSqlConnector.createTable(SCHEMA_NAME_FILES, randomName(), options, fields);
    }

    /**
     * Takes a list of function arguments and converts it to equivalent options
     * that would be used if the file was declared using DDL.
     */
    private static Map<String, Object> toOptions(List<Object> arguments, String format) {
        assert arguments.size() == PARAMETERS.size();

        Map<String, Object> options = new HashMap<>();
        options.put(OPTION_FORMAT, format);
        for (int i = 0; i < arguments.size(); i++) {
            if (arguments.get(i) != null) {
                options.put(PARAMETERS.get(i).name(), arguments.get(i));
            }
        }
        return options;
    }

    private static String randomName() {
        return SCHEMA_NAME_FILES + "_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }

    @Override
    public List<Permission> permissions(SqlCall call, HecatonSqlValidator validator) {
        // Try to find operand by name first, most common option.
        SqlNode astPath = findOperandByName(OPTION_PATH, call);
        // If not successful -> try to find by position: SELECT * FROM TABLE(fn('path', 'glob'))
        if (astPath == null && call.operandCount() > 0) {
            astPath = findOperandByPosition(0, call);
        }
        if (astPath == null) {
            throwCantDeterminePath();
        }

        if (SqlUtil.isNullLiteral(astPath, true)) {
            throwCantDeterminePath();
        }

        if (astPath instanceof SqlLiteral literal) {
            String path = extractStringValue(literal);
            if (path != null) {
                return Collections.singletonList(ConnectorPermission.file(path, ActionConstants.ACTION_READ));
            } else {
                throwCantDeterminePath();
            }
        } else if (astPath instanceof SqlDynamicParam param) {
            Object pathObj = validator.getRawArgumentAt(param.getIndex());
            if (pathObj instanceof String path) {
                return Collections.singletonList(ConnectorPermission.file(path, ActionConstants.ACTION_READ));
            } else {
                throwCantDeterminePath();
            }
        }
        return Collections.emptyList();
    }

    private void throwCantDeterminePath() {
        throw QueryException.error("Can't determine the file path, query is denied in secure environment");
    }

    private static SqlNode findOperandByName(String name, SqlCall call) {
        for (int i = 0; i < call.operandCount(); i++) {
            if (call.operand(i) instanceof SqlBasicCall) {
                SqlCall assignment = call.operand(i);
                SqlIdentifier id = assignment.operand(1);
                if (name.equals(id.getSimple())) {
                    return assignment.operand(0);
                }
            }
        }
        return null;
    }

    private static SqlNode findOperandByPosition(int pos, SqlCall call) {
        if (call.operand(pos) instanceof SqlLiteral) {
            return call.operand(pos);
        }
        return null;
    }

    private static final class FileOperandMetadata extends HecatonSqlOperandMetadata {

        private static final FileOperandMetadata INSTANCE = new FileOperandMetadata();

        private FileOperandMetadata() {
            super(
                    PARAMETERS,
                    new HecatonOperandTypeInference(PARAMETERS, new ReplaceUnknownOperandTypeInference(ANY))
            );
        }

        @Override
        protected boolean checkOperandTypes(HecatonCallBinding binding, boolean throwOnFailure) {
            return true;
        }
    }
}
