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

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.HecatonTestSupport.assertThrows;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonIntegerTypeTest {
    @Test
    public void testNullableIntegerTypeOfTypeName() {
        assertType(TINYINT, Byte.SIZE - 1, false, HecatonIntegerType.create(TINYINT, false));
        assertType(SMALLINT, Short.SIZE - 1, false, HecatonIntegerType.create(SMALLINT, false));
        assertType(INTEGER, Integer.SIZE - 1, false, HecatonIntegerType.create(INTEGER, false));
        assertType(BIGINT, Long.SIZE - 1, false, HecatonIntegerType.create(BIGINT, false));

        assertType(TINYINT, Byte.SIZE - 1, true, HecatonIntegerType.create(TINYINT, true));
        assertType(SMALLINT, Short.SIZE - 1, true, HecatonIntegerType.create(SMALLINT, true));
        assertType(INTEGER, Integer.SIZE - 1, true, HecatonIntegerType.create(INTEGER, true));
        assertType(BIGINT, Long.SIZE - 1, true, HecatonIntegerType.create(BIGINT, true));
    }

    @Test
    public void testNullableIntegerTypeOfType() {
        HecatonIntegerType intType = HecatonIntegerType.create(INTEGER, false);
        HecatonIntegerType nullableIntType = HecatonIntegerType.create(INTEGER, true);

        assertSame(intType, HecatonIntegerType.create(intType, false));
        assertSame(nullableIntType, HecatonIntegerType.create(intType, true));

        assertSame(intType, HecatonIntegerType.create(nullableIntType, false));
        assertSame(nullableIntType, HecatonIntegerType.create(nullableIntType, true));
    }

    @Test
    public void testNullableIntegerTypeOfBitWidth() {
        for (int i = 0; i < Long.SIZE + 10; ++i) {
            RelDataType type = HecatonIntegerType.create(i, false);
            RelDataType nullableType = HecatonIntegerType.create(i, true);

            if (i < Byte.SIZE) {
                assertType(TINYINT, i, false, type);
                assertType(TINYINT, i, true, nullableType);
            } else if (i < Short.SIZE) {
                assertType(SMALLINT, i, false, type);
                assertType(SMALLINT, i, true, nullableType);
            } else if (i < Integer.SIZE) {
                assertType(INTEGER, i, false, type);
                assertType(INTEGER, i, true, nullableType);
            } else if (i < Long.SIZE) {
                assertType(BIGINT, i, false, type);
                assertType(BIGINT, i, true, nullableType);
            } else {
                assertType(BIGINT, Long.SIZE, false, type);
                assertType(BIGINT, Long.SIZE, true, nullableType);
            }
        }
    }

    @Test
    public void testBitWidthOfLong() {
        assertEquals(0, HecatonIntegerType.bitWidthOf(0));
        assertEquals(1, HecatonIntegerType.bitWidthOf(1));
        assertEquals(1, HecatonIntegerType.bitWidthOf(-1));
        assertEquals(2, HecatonIntegerType.bitWidthOf(2));
        assertEquals(2, HecatonIntegerType.bitWidthOf(-2));
        assertEquals(10, HecatonIntegerType.bitWidthOf(555));
        assertEquals(10, HecatonIntegerType.bitWidthOf(-555));

        assertEquals(Long.SIZE - 1, HecatonIntegerType.bitWidthOf(Long.MAX_VALUE));
        assertEquals(Long.SIZE - 1, HecatonIntegerType.bitWidthOf(Long.MIN_VALUE));

        assertEquals(Integer.SIZE - 1, HecatonIntegerType.bitWidthOf(Integer.MAX_VALUE));
        assertEquals(Integer.SIZE - 1, HecatonIntegerType.bitWidthOf(Integer.MIN_VALUE));

        assertEquals(Short.SIZE - 1, HecatonIntegerType.bitWidthOf(Short.MAX_VALUE));
        assertEquals(Short.SIZE - 1, HecatonIntegerType.bitWidthOf(Short.MIN_VALUE));

        assertEquals(Byte.SIZE - 1, HecatonIntegerType.bitWidthOf(Byte.MAX_VALUE));
        assertEquals(Byte.SIZE - 1, HecatonIntegerType.bitWidthOf(Byte.MIN_VALUE));
    }

    @Test
    public void testBitWidthOfTypeName() {
        for (SqlTypeName typeName : SqlTypeName.values()) {
            switch (typeName) {
                case TINYINT:
                    assertEquals(Byte.SIZE - 1, HecatonIntegerType.bitWidthOf(typeName));
                    break;

                case SMALLINT:
                    assertEquals(Short.SIZE - 1, HecatonIntegerType.bitWidthOf(typeName));
                    break;

                case INTEGER:
                    assertEquals(Integer.SIZE - 1, HecatonIntegerType.bitWidthOf(typeName));
                    break;

                case BIGINT:
                    assertEquals(Long.SIZE - 1, HecatonIntegerType.bitWidthOf(typeName));
                    break;

                default:
                    //noinspection ResultOfMethodCallIgnored
                    assertThrows(IllegalArgumentException.class, () -> HecatonIntegerType.bitWidthOf(typeName));
                    break;
            }
        }
    }

    private static void assertType(SqlTypeName expectedTypeName, int expectedBitWidth, boolean expectedNullable,
                                   RelDataType actual) {
        if (actual == null) {
            fail("non-null actual type expected");
        }

        assertEquals(expectedTypeName, actual.getSqlTypeName());
        assertEquals(expectedBitWidth, ((HecatonIntegerType) actual).getBitWidth());
        assertEquals(expectedNullable, actual.isNullable());
    }

}
