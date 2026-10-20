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

import org.openrewrite.Recipe;
import org.openrewrite.gradle.ChangeDependency;
import org.openrewrite.maven.ChangeDependencyGroupIdAndArtifactId;
import org.openrewrite.maven.ChangeManagedDependencyGroupIdAndArtifactId;

import java.util.ArrayList;
import java.util.List;

/**
 * Migrates an application's Java code and Maven or Gradle build from Hazelcast 5.7 to Hecaton
 * (spec §5). Configuration and other non-Java files are migrated by the {@code hecaton-migrate}
 * command.
 */
public class FromHazelcast extends Recipe {

    @Override
    public String getDisplayName() {
        return "Migrate from Hazelcast to Hecaton";
    }

    @Override
    public String getDescription() {
        return "Renames the Hazelcast API to the Hecaton API (`com.hazelcast` → `uk.org.hecaton`, \"Hazelcast\" → "
                + "\"Hecaton\" in type, method and constant names), renames property keys and file names in string "
                + "literals, and replaces Hazelcast dependencies with Hecaton ones.";
    }

    @Override
    public List<Recipe> getRecipeList() {
        List<Recipe> recipes = new ArrayList<>();
        recipes.add(new RenameHazelcastJavaNames());
        recipes.add(new RenameHazelcastStrings());
        for (HecatonArtifacts.Artifact a : HecatonArtifacts.ARTIFACTS) {
            recipes.add(new ChangeDependencyGroupIdAndArtifactId(a.hazelcastGroupId(), a.hazelcastArtifactId(),
                    a.hecatonGroupId(), a.hecatonArtifactId(), HecatonArtifacts.VERSION, null));
            recipes.add(new ChangeManagedDependencyGroupIdAndArtifactId(a.hazelcastGroupId(), a.hazelcastArtifactId(),
                    a.hecatonGroupId(), a.hecatonArtifactId(), HecatonArtifacts.VERSION));
            recipes.add(new ChangeDependency(a.hazelcastGroupId(), a.hazelcastArtifactId(),
                    a.hecatonGroupId(), a.hecatonArtifactId(), HecatonArtifacts.VERSION, null, null));
        }
        return recipes;
    }
}
