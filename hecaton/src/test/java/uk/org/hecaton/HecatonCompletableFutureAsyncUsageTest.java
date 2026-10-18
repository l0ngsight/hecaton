/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton;

import uk.org.hecaton.test.archunit.ArchUnitRules;
import uk.org.hecaton.test.archunit.ArchUnitTestSupport;
import uk.org.hecaton.test.archunit.ModuleImportOptions;
import com.tngtech.archunit.core.domain.JavaClasses;
import org.junit.Test;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.conditions.ArchPredicates.are;

public class HecatonCompletableFutureAsyncUsageTest extends ArchUnitTestSupport {

    @Test
    public void noClassUsesCompletableFuture() {
        String basePackage = "uk.org.hecaton";
        JavaClasses classes = ModuleImportOptions.getCurrentModuleClasses(basePackage);

        JavaClasses javaClassesFiltered = classes
                .that(are(not(resideInAPackage("uk.org.hecaton.jet.."))));
        ArchUnitRules.COMPLETABLE_FUTURE_USED_ONLY_WITH_EXPLICIT_EXECUTOR.check(javaClassesFiltered);
    }
}
