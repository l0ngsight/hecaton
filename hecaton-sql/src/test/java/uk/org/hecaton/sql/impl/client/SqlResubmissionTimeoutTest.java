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

package uk.org.hecaton.sql.impl.client;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientSqlResubmissionMode;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.TimeUnit;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SqlResubmissionTimeoutTest extends SqlResubmissionTestSupport {
    private static final int FAILING_MAP_SIZE = 100;

    @Parameterized.Parameter
    public Integer timeout;

    @Parameterized.Parameters(name = "timeout:{0}")
    public static Object[] parameters() {
        return new Object[]{1, 10, 30};
    }

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void when_totalTimeOfResubmissionEnds_then_fail() {
        createMap(instance(), FAILING_MAP_NAME, FAILING_MAP_SIZE, FailingDuringFieldAccessObject::new,
                FailingDuringFieldAccessObject.class);

        HecatonInstance client = createClient();

        long startTime = System.currentTimeMillis();
        SqlStatement statement = new SqlStatement("update " + FAILING_MAP_NAME + " set field = field + 1");
        assertThrows(HecatonSqlException.class, () -> client.getSql().execute(statement));
        long duration = System.currentTimeMillis() - startTime;

        assertGreaterOrEquals("timeout", TimeUnit.MILLISECONDS.toSeconds(duration), timeout);

        client.shutdown();
    }

    private HecatonInstance createClient() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hecaton.client.invocation.timeout.seconds", timeout + "");
        clientConfig.getSqlConfig().setResubmissionMode(ClientSqlResubmissionMode.RETRY_ALL);
        return factory().newHecatonClient(clientConfig);
    }
}
