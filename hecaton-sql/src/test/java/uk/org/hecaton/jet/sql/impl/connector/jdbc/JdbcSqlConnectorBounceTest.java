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

package uk.org.hecaton.jet.sql.impl.connector.jdbc;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.test.ChangeLoggingRule;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.bounce.BounceMemberRule;
import uk.org.hecaton.test.bounce.BounceTestConfiguration;
import uk.org.hecaton.test.jdbc.H2DatabaseProvider;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;

import java.util.Properties;

import static uk.org.hecaton.jet.sql.SqlTestSupport.assertRowsAnyOrder;
import static uk.org.hecaton.jet.sql.SqlTestSupport.randomName;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class JdbcSqlConnectorBounceTest {

    private static final int ITEM_COUNT = 5;
    private static final int CONCURRENCY = 10;
    private static final String TEST_DATABASE_REF = "test_database_ref";

    private String tableName;

    private final DatabaseRule dbRule = new DatabaseRule(new H2DatabaseProvider());
    private final BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(getConfig(dbRule.getDbConnectionUrl()))
                .clusterSize(4)
                .driverCount(4)
                .driverType(BounceTestConfiguration.DriverType.CLIENT)
                .build();
    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-jdbc-sql-connector-bounce.xml");

    @Rule
    public RuleChain chain = RuleChain.outerRule(dbRule).around(bounceMemberRule);

    private Config getConfig(String jdbcUrl) {
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", jdbcUrl);
        return smallInstanceConfig().addDataConnectionConfig(
                new DataConnectionConfig(TEST_DATABASE_REF)
                        .setType("jdbc")
                        .setProperties(properties)
        );
    }

    @Before
    public void setUp() throws Exception {
        tableName = "table_" + randomName();
        dbRule.createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)");
        dbRule.insertItems(tableName, ITEM_COUNT);

        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );
    }

    @Test
    public void testQuery() {

        QueryRunnable[] testTasks = new QueryRunnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            testTasks[i] = new QueryRunnable(bounceMemberRule.getNextTestDriver());
        }
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(1));
    }

    private void execute(String sql, Object... arguments) {
        try (SqlResult ignored = bounceMemberRule
                .getSteadyMember()
                .getSql()
                .execute(sql, arguments)
        ) {
            // empty try-with-resources
        }
    }

    public class QueryRunnable implements Runnable {

        private final HecatonInstance hecatonInstance;

        public QueryRunnable(HecatonInstance hecatonInstance) {
            this.hecatonInstance = hecatonInstance;
        }

        @Override
        public void run() {
            assertRowsAnyOrder(
                    hecatonInstance,
                    "SELECT * FROM " + tableName,
                    newArrayList(
                            new SqlTestSupport.Row(0, "name-0"),
                            new SqlTestSupport.Row(1, "name-1"),
                            new SqlTestSupport.Row(2, "name-2"),
                            new SqlTestSupport.Row(3, "name-3"),
                            new SqlTestSupport.Row(4, "name-4")
                    )
            );
        }
    }
}
