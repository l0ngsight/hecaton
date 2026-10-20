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

package uk.org.hecaton.jet.sql.impl.validate.operators.special;

import uk.org.hecaton.jet.sql.impl.parse.SqlExplainStatement;
import uk.org.hecaton.jet.sql.impl.validate.HecatonCallBinding;
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonSpecialOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;

/**
 * Hecaton implementation of EXPLAIN operator.
 */
public class HecatonExplainOperator extends HecatonSpecialOperator {
    public HecatonExplainOperator() {
        super("EXPLAIN", SqlKind.EXPLAIN);
    }

    @Override
    protected boolean checkOperandTypes(HecatonCallBinding callBinding, boolean throwOnFailure) {
        throw new UnsupportedOperationException("Never called for EXPLAIN");
    }

    @Override
    public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
        assert operands != null;
        return new SqlExplainStatement(pos, operands[0]);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.keyword("EXPLAIN PLAN FOR");
        SqlExplainStatement explainStatement = (SqlExplainStatement) call;
        explainStatement.getExplicandum().unparse(writer, leftPrec, rightPrec);
    }
}
