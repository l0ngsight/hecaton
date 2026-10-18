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

package uk.org.hecaton.jet.sql.impl.validate.types;

import uk.org.hecaton.jet.sql.impl.validate.types.HecatonObjectType.Field;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonObjectTypeTest {
    private static final HecatonTypeFactory TYPE_FACTORY = HecatonTypeFactory.INSTANCE;

    @Test
    public void testDigestEscaping() {
        HecatonObjectType person = new HecatonObjectType("P(e:r=s,o)n");
        person.addField(new Field("name", 0, nullable(VARCHAR)));
        person.addField(new Field("(:fri=end,)", 1, person));
        HecatonObjectType.finalizeFields(List.of(person));

        assertEquals("P\\(e\\:r=s\\,o\\)n("
                + "name:VARCHAR CHARACTER SET \"UTF-16LE\", "
                + "\\(\\:fri=end\\,\\):P\\(e\\:r=s\\,o\\)n"
                + ")", person.getFullTypeString());
    }

    @Test
    public void testFinalization() {
        HecatonObjectType person = new HecatonObjectType("Person");
        person.addField(new Field("name", 0, nullable(VARCHAR)));
        person.addField(new Field("age", 1, nullable(INTEGER)));

        assertThatThrownBy(person::getFieldList).hasMessage("Type fields are not finalized");
        assertThatThrownBy(() -> person.getField("name", false, false))
                .hasMessage("Type fields are not finalized");
        assertThatThrownBy(person::getFieldNames).hasMessage("Type fields are not finalized");
        assertThatThrownBy(person::getFieldCount).hasMessage("Type fields are not finalized");

        HecatonObjectType.finalizeFields(List.of(person));

        assertThat(person.getFieldList()).hasSize(2);
        assertThat(person.getField("name", false, false)).isNotNull();
        assertThat(person.getFieldNames()).containsExactly("name", "age");
        assertThat(person.getFieldCount()).isEqualTo(2);
        assertThatThrownBy(() -> person.addField(new Field("friend", 2, person)))
                .hasMessage("Type fields are already finalized");
        assertThatThrownBy(() -> HecatonObjectType.finalizeFields(List.of(person)))
                .hasMessage("Type fields are already finalized");
    }

    private static RelDataType nullable(SqlTypeName typeName) {
        return TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(typeName), true);
    }
}
