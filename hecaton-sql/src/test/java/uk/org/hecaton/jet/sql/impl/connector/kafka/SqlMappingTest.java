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

package uk.org.hecaton.jet.sql.impl.connector.kafka;

import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
public class SqlMappingTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void testExternalNameShouldNotHaveMoreComponents() {
        String query = "CREATE MAPPING topic EXTERNAL NAME \"schema1\".\"topic\" ("
                + "__key INT,"
                + "this VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ")";
        assertThatThrownBy(() -> sqlService.execute(query))
                .isInstanceOf(HecatonSqlException.class)
                .hasMessageContaining("Invalid external name \"schema1\".\"topic\"");
    }

}
