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

package uk.org.hecaton.internal.config;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.HotRestartPersistenceConfig;
import uk.org.hecaton.config.PersistenceConfig;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Collection;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PersistenceAndHotRestartPersistenceMergerTest {

    static final String HECATON_START_TAG = "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\">\n";
    static final String HECATON_END_TAG = "</hecaton>\n";

    private static final String XML = HECATON_START_TAG
            + "<persistence enabled=\"%s\">"
            + "    <base-dir>%s</base-dir>"
            + "    <parallelism>%d</parallelism>"
            + "</persistence>\n"
            + "<hot-restart-persistence enabled=\"%s\">"
            + "    <base-dir>%s</base-dir>"
            + "    <parallelism>%d</parallelism>"
            + "</hot-restart-persistence>\n"
            + HECATON_END_TAG;

    private static final String YAML = ""
            + "hecaton:\n"
            + "  persistence:\n"
            + "    enabled: %s\n"
            + "    base-dir: %s\n"
            + "    parallelism: %d\n"
            + "  hot-restart-persistence:\n"
            + "    enabled: %s\n"
            + "    base-dir: %s\n"
            + "    parallelism: %d\n";


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        String dir = HecatonTestSupport.randomString();
        int parallelism = abs(dir.hashCode());

        return asList(new Object[][] {
                {true, true, dir, dir, parallelism, parallelism},
                {true, false, dir, dir, parallelism, parallelism},
                {false, true, dir, dir + "a", parallelism, max(parallelism - 1, parallelism + 1)},
                {false, false, dir, dir, parallelism, parallelism}
        });
    }

    @Parameterized.Parameter
    public boolean persistenceEnabled;

    @Parameterized.Parameter(1)
    public boolean hotRestartPersistenceEnabled;

    @Parameterized.Parameter(2)
    public String directory;

    @Parameterized.Parameter(3)
    public String expectedDirectory;

    @Parameterized.Parameter(4)
    public int parallelism;

    @Parameterized.Parameter(5)
    public int expectedParallelism;

    @Test
    public void testMergePersistenceAndHotRestartPersistenceXml() {
        test(XML);
    }

    @Test
    public void testMergePersistenceAndHotRestartPersistenceYaml() {
        test(YAML);
    }

    private void test(String template) {
        String xml = String.format(template,
                persistenceEnabled, directory, parallelism,
                hotRestartPersistenceEnabled, directory + "a", max(parallelism - 1, parallelism + 1));

        Config cfg = Config.loadFromString(xml);
        HotRestartPersistenceConfig hotRestartPersistenceConfig = cfg.getHotRestartPersistenceConfig();
        PersistenceConfig persistenceConfig = cfg.getPersistenceConfig();

        assertEquals(persistenceEnabled || hotRestartPersistenceEnabled, persistenceConfig.isEnabled());
        assertEquals(new File(expectedDirectory).getAbsolutePath(), persistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(expectedParallelism, persistenceConfig.getParallelism());

        assertEquals(persistenceEnabled || hotRestartPersistenceEnabled, hotRestartPersistenceConfig.isEnabled());
        assertEquals(new File(expectedDirectory).getAbsolutePath(),
                hotRestartPersistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(expectedParallelism, hotRestartPersistenceConfig.getParallelism());
    }
}
