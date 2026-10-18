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

package uk.org.hecaton.jet.sql.impl;

import uk.org.hecaton.jet.sql.impl.validate.types.HecatonIntegerType;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeFactory;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeUtils;
import uk.org.hecaton.sql.impl.type.QueryDataTypeFamily;
import uk.org.hecaton.sql.impl.type.converter.Converter;
import uk.org.hecaton.sql.impl.type.converter.Converters;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;

/**
 * Custom Hecaton expression builder.
 * <p>
 * Currently, this custom expression builder is used just to workaround quirks
 * of the default Calcite expression builder.
 */
public final class HecatonRexBuilder extends RexBuilder {

    public static final HecatonRexBuilder INSTANCE = new HecatonRexBuilder();

    private HecatonRexBuilder() {
        super(HecatonTypeFactory.INSTANCE);
    }

    @Override
    public RexNode makeLiteral(Object value, RelDataType type, boolean allowCast) {
        // Make sure that numeric literals get a correct return type during the conversion.
        // Without this code, Apache Calcite may assign incorrect types to some literals during conversion.
        // For example, new BigDecimal(Long.MAX_VALUE + "1") will receive the BIGINT type.
        // To see the problem in action, you may comment out this code and run CastFunctionIntegrationTest.
        // Some conversions will fail due to precision loss.

        if (type.getSqlTypeName() == ANY && value instanceof Number number) {
            Converter converter = Converters.getConverter(value.getClass());

            if (converter != null) {
                QueryDataTypeFamily typeFamily = converter.getTypeFamily();

                if (typeFamily.isNumericInteger()) {
                    int bitWidth = HecatonIntegerType.bitWidthOf(number.longValue());
                    type = HecatonIntegerType.create(bitWidth, false);
                } else {
                    SqlTypeName typeName = HecatonTypeUtils.toCalciteType(typeFamily);

                    type = HecatonTypeFactory.INSTANCE.createSqlType(typeName);
                }
            }
        }

        // There are the Calcite bug with infinite recursion during non-nullable NULL type construction.
        // Problem description : in a query like SELECT NULL BETWEEN NULL AND NULL.
        // RexBuilder calls makeLiteral(null, BasicSqlType, true). makeLiteral() tries to construct
        // non-nullable literal, then check isNullable property (which is true on each recursive iteration)
        // and launch recursive call again. Of course, StackOverflow happens.
        // Generally, Calcite tries to create NULL literal of non-nullable type. /shrug

        return super.makeLiteral(value, type, allowCast);
    }
}
