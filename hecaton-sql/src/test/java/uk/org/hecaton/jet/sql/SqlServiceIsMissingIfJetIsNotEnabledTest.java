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

package uk.org.hecaton.jet.sql;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.impl.exception.JetDisabledException;
import uk.org.hecaton.sql.impl.InternalSqlService;
import uk.org.hecaton.sql.impl.MissingSqlService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlServiceIsMissingIfJetIsNotEnabledTest extends HecatonTestSupport {

    @Test
    public void test() {
        Config config = new Config();
        config.getJetConfig().setEnabled(false);

        HecatonInstance hecatonInstance = createHecatonInstance(config);
        InternalSqlService sqlService = (InternalSqlService) hecatonInstance.getSql();

        assertInstanceOf(MissingSqlService.class, sqlService);

        // HZ-3438
        assertThrows(JetDisabledException.class, () -> sqlService.mappingDdl("abxy"));
    }
}
