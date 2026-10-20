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

package uk.org.hecaton.jet.sql.impl.validate.types;

import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;

/**
 * Tests for type mapping between Hecaton and Apache Calcite.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonTypeUtilsTest {
    @Test
    public void testHecatonToCalcite() {
        assertSame(SqlTypeName.VARCHAR, HecatonTypeUtils.toCalciteType(QueryDataType.VARCHAR));

        assertSame(SqlTypeName.BOOLEAN, HecatonTypeUtils.toCalciteType(QueryDataType.BOOLEAN));

        assertSame(SqlTypeName.TINYINT, HecatonTypeUtils.toCalciteType(QueryDataType.TINYINT));
        assertSame(SqlTypeName.SMALLINT, HecatonTypeUtils.toCalciteType(QueryDataType.SMALLINT));
        assertSame(SqlTypeName.INTEGER, HecatonTypeUtils.toCalciteType(QueryDataType.INT));
        assertSame(SqlTypeName.BIGINT, HecatonTypeUtils.toCalciteType(QueryDataType.BIGINT));

        assertSame(SqlTypeName.DECIMAL, HecatonTypeUtils.toCalciteType(QueryDataType.DECIMAL));

        assertSame(SqlTypeName.REAL, HecatonTypeUtils.toCalciteType(QueryDataType.REAL));
        assertSame(SqlTypeName.DOUBLE, HecatonTypeUtils.toCalciteType(QueryDataType.DOUBLE));

        assertSame(SqlTypeName.DATE, HecatonTypeUtils.toCalciteType(QueryDataType.DATE));
        assertSame(SqlTypeName.TIME, HecatonTypeUtils.toCalciteType(QueryDataType.TIME));
        assertSame(SqlTypeName.TIMESTAMP, HecatonTypeUtils.toCalciteType(QueryDataType.TIMESTAMP));
        assertSame(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, HecatonTypeUtils.toCalciteType(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME));
    }

    @Test
    public void testCalciteToHecaton() {
        assertSame(QueryDataType.JSON, HecatonTypeUtils.toHecatonType(HecatonJsonType.TYPE));
        assertSame(QueryDataType.JSON, HecatonTypeUtils.toHecatonType(HecatonJsonType.TYPE_NULLABLE));

        assertSame(QueryDataType.VARCHAR, HecatonTypeUtils.toHecatonType(type(SqlTypeName.VARCHAR)));

        assertSame(QueryDataType.BOOLEAN, HecatonTypeUtils.toHecatonType(type(SqlTypeName.BOOLEAN)));

        assertSame(QueryDataType.TINYINT, HecatonTypeUtils.toHecatonType(type(SqlTypeName.TINYINT)));
        assertSame(QueryDataType.SMALLINT, HecatonTypeUtils.toHecatonType(type(SqlTypeName.SMALLINT)));
        assertSame(QueryDataType.INT, HecatonTypeUtils.toHecatonType(type(SqlTypeName.INTEGER)));
        assertSame(QueryDataType.BIGINT, HecatonTypeUtils.toHecatonType(type(SqlTypeName.BIGINT)));

        assertSame(QueryDataType.DECIMAL, HecatonTypeUtils.toHecatonType(type(SqlTypeName.DECIMAL)));

        assertSame(QueryDataType.REAL, HecatonTypeUtils.toHecatonType(type(SqlTypeName.REAL)));
        assertSame(QueryDataType.DOUBLE, HecatonTypeUtils.toHecatonType(type(SqlTypeName.DOUBLE)));

        assertSame(QueryDataType.DATE, HecatonTypeUtils.toHecatonType(type(SqlTypeName.DATE)));
        assertSame(QueryDataType.TIME, HecatonTypeUtils.toHecatonType(type(SqlTypeName.TIME)));
        assertSame(QueryDataType.TIMESTAMP, HecatonTypeUtils.toHecatonType(type(SqlTypeName.TIMESTAMP)));
        assertSame(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, HecatonTypeUtils.toHecatonType(type(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)));
    }

    private RelDataType type(SqlTypeName typeName) {
        return HecatonTypeFactory.INSTANCE.createSqlType(typeName);
    }
}
