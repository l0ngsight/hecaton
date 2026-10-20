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

package uk.org.hecaton.internal.partition;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.test.ChangeLoggingRule;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static uk.org.hecaton.spi.properties.ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED;
import static uk.org.hecaton.test.OverridePropertyRule.set;
import static java.util.Arrays.asList;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
// related issue https://github.com/hazelcast/hazelcast/issues/5444
public class MigrationCorrectnessTest extends AbstractMigrationCorrectnessTest {
    @Parameter(3)
    public boolean join_async;

    @Rule
    public final OverridePropertyRule overrideAsyncJoinPropertyRule = set(ASYNC_JOIN_STRATEGY_ENABLED.getName(), Boolean.toString(join_async));

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-trace-migrations.xml");

    @Parameters(name = "backups:{0},nodes:{1},fragmented:{2},join_async:{3}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                // sync join strategy
                {1, 2, true, false},
                {1, 2, false, false},
                {2, 3, true, false},
                {3, 4, true, false},
                {3, 4, false, false},
                // async join strategy
                {1, 2, true, true},
                {1, 2, false, true},
                {2, 3, true, true},
                {3, 4, true, true},
                {3, 4, false, true},
        });
    }

    @Override
    protected Config getConfig() {
        // Partition count is overwritten back to PartitionCorrectnessTestSupport.partitionCount
        // in PartitionCorrectnessTestSupport.getConfig(boolean, boolean).
        return smallInstanceConfig();
    }

}
