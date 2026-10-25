/*
 * Copyright (c) 2026, The Hecaton Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.clickhouse.impl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A table name, optionally with its database, and its quoted form for SQL.
 */
public record TableName(@Nullable String database, @Nonnull String table) {

    /**
     * Parses {@code table} or {@code database.table}. Names with dots can be quoted with back
     * quotes, as in {@code `my.db`.`my.table`}.
     */
    public static TableName parse(@Nonnull String name) {
        String stripped = name.strip();
        int dot = splitPoint(stripped);
        if (dot < 0) {
            return new TableName(null, unquote(stripped));
        }
        return new TableName(unquote(stripped.substring(0, dot)), unquote(stripped.substring(dot + 1)));
    }

    private static int splitPoint(String name) {
        boolean quoted = false;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '`') {
                quoted = !quoted;
            } else if (c == '.' && !quoted) {
                return i;
            }
        }
        return -1;
    }

    private static String unquote(String part) {
        String p = part.strip();
        if (p.length() >= 2 && p.startsWith("`") && p.endsWith("`")) {
            return p.substring(1, p.length() - 1).replace("``", "`");
        }
        return p;
    }

    /**
     * Quotes an identifier for ClickHouse SQL.
     */
    public static String quote(String identifier) {
        return '`' + identifier.replace("\\", "\\\\").replace("`", "\\`") + '`';
    }

    /**
     * Returns the name as it is written in SQL, quoted.
     */
    public String sql() {
        return database == null ? quote(table) : quote(database) + '.' + quote(table);
    }

    /**
     * Returns the name unquoted, as the ClickHouse client takes it for inserts.
     */
    public String plain() {
        return database == null ? table : database + '.' + table;
    }

    @Override
    public String toString() {
        return plain();
    }
}
