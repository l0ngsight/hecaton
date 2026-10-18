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

package uk.org.hecaton.jet.sql.impl.connector.jdbc.mssql;

import uk.org.hecaton.jet.sql.impl.connector.jdbc.DefaultTypeResolver;
import uk.org.hecaton.jet.sql.impl.connector.jdbc.TypeResolver;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonConcatOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonStringFunction;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;

import java.util.Locale;

/**
 * Custom dialect for MSSQL which allows correct unparsing of MSSQL specific operators, like CONCAT
 */
public class HecatonMSSQLDialect extends MssqlSqlDialect implements TypeResolver {

    /**
     * Creates a HecatonMSSQLDialect.
     */
    public HecatonMSSQLDialect(Context context) {
        super(context);
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        switch (call.getKind()) {
            case OTHER:
                if (call.getOperator() instanceof HecatonConcatOperator) {
                    unparseConcat(writer, call);
                    break;
                } else {
                    super.unparseCall(writer, call, leftPrec, rightPrec);
                    break;
                }

            case OTHER_FUNCTION:
                if (isLengthFunction(call)) {
                    unparseLength(writer, call);
                    break;
                } else {
                    super.unparseCall(writer, call, leftPrec, rightPrec);
                    break;
                }
            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    private void unparseConcat(SqlWriter writer, SqlCall call) {
        writer.print("CONCAT");
        Frame frame = writer.startList(FrameTypeEnum.PARENTHESES, "(", ")");
        for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    private static boolean isLengthFunction(SqlCall basicCall) {
        return basicCall.getOperator() instanceof HecatonStringFunction
                && basicCall.getOperator().getName().equals("LENGTH");
    }

    private void unparseLength(SqlWriter writer, SqlCall call) {
        writer.print("LEN");
        Frame frame = writer.startList(FrameTypeEnum.PARENTHESES, "(", ")");
        for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    @Override
    public QueryDataType resolveType(String columnTypeName, int precision, int scale) {
        return switch (columnTypeName.toUpperCase(Locale.ROOT)) {
            case "FLOAT" -> QueryDataType.DOUBLE;
            case "DATETIME2", "SMALLDATETIME" -> QueryDataType.TIMESTAMP;
            default -> DefaultTypeResolver.resolveType(columnTypeName, precision, scale);
        };
    }
}
