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

package uk.org.hecaton.jet.sql.impl.connector.file;

import uk.org.hecaton.sql.impl.schema.MappingField;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CsvResolverTest {

    @Test
    public void test_resolveFields() {
        // given
        Set<String> headers = Set.of("field1", "field2");

        // when
        List<MappingField> fields = CsvResolver.resolveFields(headers);

        // then
        assertThat(fields)
                .hasSize(2)
                .contains(new MappingField("field1", QueryDataType.VARCHAR))
                .contains(new MappingField("field2", QueryDataType.VARCHAR));
    }
}
