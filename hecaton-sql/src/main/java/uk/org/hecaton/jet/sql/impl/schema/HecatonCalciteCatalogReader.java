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

package uk.org.hecaton.jet.sql.impl.schema;

import uk.org.hecaton.jet.sql.impl.connector.virtual.ViewTable;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonJsonType;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeUtils;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

/**
 * Custom catalog reader that allows for setting predefined schema paths and wrapping of returned tables.
 */
public class HecatonCalciteCatalogReader extends CalciteCatalogReader {

    public HecatonCalciteCatalogReader(
            CalciteSchema rootSchema,
            List<List<String>> schemaPaths,
            RelDataTypeFactory typeFactory,
            CalciteConnectionConfig config
    ) {
        // Call the protected constructor that is not visible otherwise.
        super(
                rootSchema,
                SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
                schemaPaths,
                typeFactory,
                config
        );
    }

    /**
     * Hook into the Apache Calcite table creation process and wrap the Calcite table into our own implementation
     * that resolves signatures of tables with pushed-down projects and filters properly.
     */
    @Override
    public Prepare.PreparingTable getTable(List<String> names) {
        // Resolve the original table as usual.
        Prepare.PreparingTable table = super.getTable(names);

        if (table == null) {
            return null;
        }

        HecatonTable hzTable = table.unwrap(HecatonTable.class);
        assert hzTable != null;
        if (hzTable.getTarget() instanceof ViewTable) {
            return new HecatonViewRelOptTable(table, ((ViewTable) hzTable.getTarget()).getViewRel());
        }

        // Wrap it into our own table.
        return new HecatonRelOptTable(table);
    }

    @Override
    public RelDataType getNamedType(SqlIdentifier typeName) {
        if (HecatonTypeUtils.isObjectIdentifier(typeName)) {
            return typeFactory.createSqlType(SqlTypeName.ANY);
        }

        if (HecatonTypeUtils.isJsonIdentifier(typeName)) {
            return HecatonJsonType.create(true);
        }

        return super.getNamedType(typeName);
    }
}
