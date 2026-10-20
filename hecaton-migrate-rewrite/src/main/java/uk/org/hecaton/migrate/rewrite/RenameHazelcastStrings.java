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

package uk.org.hecaton.migrate.rewrite;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import uk.org.hecaton.migrate.files.TextMigrator;

/**
 * Renames the Hazelcast names in Java string literals that Hecaton reads under a new name:
 * property keys ({@code "hazelcast.logging.type"}), config file names, {@code com.hazelcast}
 * class names, {@code HZ_} environment variables, the Spring Boot config key and the SQL
 * catalog. The rules are the ones {@code hecaton-migrate} applies to configuration files
 * ({@link TextMigrator#migrateString}), so code and configuration stay consistent; an
 * application's own names are left alone.
 */
public class RenameHazelcastStrings extends Recipe {

    @Override
    public String getDisplayName() {
        return "Rename Hazelcast property keys and file names in string literals";
    }

    @Override
    public String getDescription() {
        return "Renames `hazelcast.*` property keys, config file names, class names, `HZ_` environment variables "
                + "and the SQL catalog in string literals to their Hecaton names.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<ExecutionContext>() {
            @Override
            public J.Literal visitLiteral(J.Literal literal, ExecutionContext ctx) {
                J.Literal l = super.visitLiteral(literal, ctx);
                if (!(l.getValue() instanceof String) || l.getValueSource() == null) {
                    return l;
                }
                String value = (String) l.getValue();
                String renamed = TextMigrator.migrateString(value);
                if (renamed.equals(value)) {
                    return l;
                }
                return l.withValue(renamed).withValueSource(TextMigrator.migrateString(l.getValueSource()));
            }
        };
    }
}
