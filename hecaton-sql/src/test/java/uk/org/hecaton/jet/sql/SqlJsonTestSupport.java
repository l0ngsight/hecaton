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

package uk.org.hecaton.jet.sql;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;
import uk.org.hecaton.internal.json.ParseException;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.SqlRowMetadata;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import static uk.org.hecaton.jet.Util.entry;
import static uk.org.hecaton.jet.core.test.TestSupport.SAME_ITEMS_ANY_ORDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
public abstract class SqlJsonTestSupport extends SqlTestSupport {
    private static final JsonMapper SERIALIZER = new JsonMapper();

    public static HecatonJsonValue json(final String value) {
        return new HecatonJsonValue(value);
    }

    public static HecatonJsonValue jsonArray(Object... values) {
        return json(jsonString(values));
    }

    public static Object querySingleValue(final String sql) {
        final List<Map<String, Object>> rows = query(sql);
        assertEquals(1, rows.size());

        final Map<String, Object> row = rows.get(0);
        assertEquals(1, row.size());

        return row.values().iterator().next();
    }

    public static List<Map<String, Object>> query(final String sql) {
        final List<Map<String, Object>> results = new ArrayList<>();

        for (final SqlRow row : instance().getSql().execute(sql)) {
            final Map<String, Object> result = new HashMap<>();
            final SqlRowMetadata rowMetadata = row.getMetadata();
            for (int i = 0; i < rowMetadata.getColumnCount(); i++) {
                result.put(rowMetadata.getColumn(i).getName(), row.getObject(i));
            }

            results.add(result);
        }

        return results;
    }

    public static Map<Object, Object> objectMap(Object... kvPairs) {
        final Map<Object, Object> result = new LinkedHashMap<>();

        for (int i = 0; i < kvPairs.length; i += 2) {
            result.put(kvPairs[i], kvPairs[i + 1]);
        }

        return result;
    }

    public static String jsonString(Object value) {
        try {
            return SERIALIZER.writeValueAsString(value);
        } catch (JacksonException e) {
            throw new HecatonException("Unable to serialize value: ", e);
        }
    }

    protected void assertJsonRowsAnyOrder(String sql, Collection<Row> rows) {
        assertJsonRowsAnyOrder(sql, Collections.emptyList(), rows);
    }

    protected void assertJsonRowsAnyOrder(String sql, List<Object> params, Collection<Row> rows) {
        for (Row row : rows) {
            convertRow(row);
        }

        List<Row> actualRows = new ArrayList<>();
        try (SqlResult result = instance().getSql().execute(sql, params.toArray())) {
            result.iterator().forEachRemaining(row -> actualRows.add(convertRow(new Row(row))));
        }
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(rows);
    }

    private static Row convertRow(Row row) {
        Object[] rowObj = row.getValues();
        for (int i = 0; i < rowObj.length; i++) {
            if (rowObj[i] instanceof HecatonJsonValue value) {
                try {
                    if (Json.parse(value.getValue()) instanceof JsonObject) {
                        rowObj[i] = new JsonObjectWithRelaxedEquality(value);
                    }
                } catch (ParseException parseException) {
                    throw new HecatonException("Invalid JSON: " + value.getValue(), parseException);
                }
            }
        }
        return row;
    }

    /**
     * A JSON value with equals method that returns true for objects with
     * the same keys and values, but in any order.
     */
    protected static class JsonObjectWithRelaxedEquality {
        private final List<Map.Entry<String, JsonValue>> fields = new ArrayList<>();

        JsonObjectWithRelaxedEquality(HecatonJsonValue json) {
            JsonObject jsonObject = (JsonObject) Json.parse(json.getValue());
            jsonObject.iterator().forEachRemaining(m -> fields.add(entry(m.getName(), m.getValue())));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof JsonObjectWithRelaxedEquality jowre
                    && SAME_ITEMS_ANY_ORDER.test(fields, jowre.fields);
        }

        @Override
        public String toString() {
            return fields.toString();
        }
    }
}
