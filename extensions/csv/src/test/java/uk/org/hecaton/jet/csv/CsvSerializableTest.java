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

package uk.org.hecaton.jet.csv;

import uk.org.hecaton.jet.csv.impl.CsvReadFileFnProvider;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.archunit.ArchUnitRules;
import uk.org.hecaton.test.archunit.ArchUnitTestSupport;
import uk.org.hecaton.test.archunit.ModuleImportOptions;
import com.tngtech.archunit.core.domain.JavaClasses;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class CsvSerializableTest extends ArchUnitTestSupport {

    @Test
    public void serializable_classes_should_have_valid_serialVersionUID() {
        String basePackage = CsvReadFileFnProvider.class.getPackage().getName();
        JavaClasses classes = ModuleImportOptions.getCurrentModuleClasses(basePackage);

        ArchUnitRules.SERIALIZABLE_SHOULD_HAVE_VALID_SERIAL_VERSION_UID.check(classes);
    }

}
