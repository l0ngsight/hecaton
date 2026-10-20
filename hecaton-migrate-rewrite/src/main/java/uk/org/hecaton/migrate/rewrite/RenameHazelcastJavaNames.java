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
import org.openrewrite.ScanningRecipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeFieldName;
import org.openrewrite.java.ChangeMethodName;
import org.openrewrite.java.ChangePackage;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Flag;
import org.openrewrite.java.tree.JavaSourceFile;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.TypeUtils;
import uk.org.hecaton.migrate.HecatonNames;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Renames the Hazelcast API an application uses to the Hecaton API (spec §5.1).
 * <p>
 * The scan collects, from the type-attributed sources, every {@code com.hazelcast} type,
 * method and constant whose name contains "Hazelcast" (in any case). Each is renamed with the
 * rule the fork itself was made with ({@link HecatonNames#renameJavaName},
 * {@link HecatonNames#renameIdentifier}): methods and constants first, while their declaring
 * types still have the Hazelcast name, then types, then the {@code com.hazelcast} package.
 * Deriving the renames from the application rather than from a list keeps every Hecaton
 * module covered, extensions included.
 */
public class RenameHazelcastJavaNames extends ScanningRecipe<RenameHazelcastJavaNames.Renames> {

    private static final Pattern HAZELCAST_ANY_CASE = Pattern.compile("(?i)hazelcast");

    /** What the scan found, keyed so that the renames are applied in a stable order. */
    public static final class Renames {
        /** Method pattern (Hazelcast names) → new method name. */
        final Map<String, String> methods = new TreeMap<>();
        /** Constant: owner type (Hazelcast name) and constant name → new constant name. */
        final Map<List<String>, String> constants = new TreeMap<>(Comparator.comparing(Object::toString));
        /** Type name after the package move ({@code uk.org.hecaton.core.HazelcastInstance}) → Hecaton name. */
        final Map<String, String> types = new TreeMap<>();
    }

    @Override
    public String getDisplayName() {
        return "Rename the Hazelcast API to the Hecaton API";
    }

    @Override
    public String getDescription() {
        return "Moves `com.hazelcast` types to `uk.org.hecaton` and renames \"Hazelcast\" to \"Hecaton\" in "
                + "the names of the types, methods and constants an application uses.";
    }

    @Override
    public Renames getInitialValue(ExecutionContext ctx) {
        return new Renames();
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getScanner(Renames acc) {
        return new TreeVisitor<Tree, ExecutionContext>() {
            @Override
            public Tree visit(Tree tree, ExecutionContext ctx) {
                if (tree instanceof JavaSourceFile) {
                    scan((JavaSourceFile) tree, acc);
                }
                return tree;
            }
        };
    }

    private static void scan(JavaSourceFile cu, Renames acc) {
        for (JavaType type : cu.getTypesInUse().getTypesInUse()) {
            JavaType.FullyQualified fq = TypeUtils.asFullyQualified(type);
            if (fq != null) {
                addType(fq.getFullyQualifiedName(), acc);
            }
        }
        for (JavaType.Method method : cu.getTypesInUse().getUsedMethods()) {
            addMethod(method, acc);
        }
        for (JavaType.Method method : cu.getTypesInUse().getDeclaredMethods()) {
            // an application method that overrides a Hazelcast one (setHazelcastInstance, ...)
            Optional<JavaType.Method> overridden = TypeUtils.findOverriddenMethod(method);
            overridden.ifPresent(m -> addMethod(m, acc));
        }
        for (JavaType.Variable variable : cu.getTypesInUse().getVariables()) {
            addConstant(variable, acc);
        }
    }

    private static void addType(String fqn, Renames acc) {
        if (HecatonNames.isHazelcastJavaName(fqn)) {
            String simpleName = fqn.substring(fqn.lastIndexOf('.') + 1);
            if (HAZELCAST_ANY_CASE.matcher(simpleName).find()) {
                acc.types.put(movePackage(fqn), HecatonNames.renameJavaName(fqn));
            }
        }
    }

    private static void addMethod(JavaType.Method method, Renames acc) {
        JavaType.FullyQualified declaringType = method.getDeclaringType();
        if (declaringType != null && HecatonNames.isHazelcastJavaName(declaringType.getFullyQualifiedName())
                && HAZELCAST_ANY_CASE.matcher(method.getName()).find()) {
            acc.methods.put(MethodMatcher.methodPattern(method), HecatonNames.renameIdentifier(method.getName()));
        }
    }

    private static void addConstant(JavaType.Variable variable, Renames acc) {
        JavaType.FullyQualified owner = TypeUtils.asFullyQualified(variable.getOwner());
        if (owner != null && variable.hasFlags(Flag.Static)
                && HecatonNames.isHazelcastJavaName(owner.getFullyQualifiedName())
                && HAZELCAST_ANY_CASE.matcher(variable.getName()).find()) {
            acc.constants.put(List.of(owner.getFullyQualifiedName(), variable.getName()),
                    HecatonNames.renameIdentifier(variable.getName()));
        }
    }

    /** {@code com.hazelcast.x.HazelcastY} → {@code uk.org.hecaton.x.HazelcastY}: what ChangePackage leaves. */
    private static String movePackage(String fqn) {
        return HecatonNames.HECATON_PACKAGE + fqn.substring(HecatonNames.HAZELCAST_PACKAGE.length());
    }

    /**
     * Methods and constants are renamed in place while their types still have the Hazelcast
     * names. Moving the package before renaming types keeps the imports sorted: ChangeType then
     * adds each new import among the other {@code uk.org.hecaton} ones.
     */
    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor(Renames acc) {
        List<TreeVisitor<?, ExecutionContext>> renames = new ArrayList<>();
        acc.methods.forEach((pattern, newName) -> renames.add(new ChangeMethodName(pattern, newName, true, false).getVisitor()));
        acc.constants.forEach((constant, newName) ->
                renames.add(new ChangeFieldName<>(constant.get(0), constant.get(1), newName)));
        renames.add(new ChangePackage(HecatonNames.HAZELCAST_PACKAGE, HecatonNames.HECATON_PACKAGE, true).getVisitor());
        acc.types.forEach((from, to) -> renames.add(new ChangeType(from, to, false).getVisitor()));
        return new TreeVisitor<Tree, ExecutionContext>() {
            @Override
            public Tree visit(Tree tree, ExecutionContext ctx) {
                if (!(tree instanceof JavaSourceFile)) {
                    return tree;
                }
                Tree result = tree;
                for (TreeVisitor<?, ExecutionContext> rename : renames) {
                    result = rename.visit(result, ctx);
                }
                return result;
            }
        };
    }
}
