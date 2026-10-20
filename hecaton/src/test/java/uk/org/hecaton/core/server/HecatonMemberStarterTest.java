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

package uk.org.hecaton.core.server;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class HecatonMemberStarterTest
        extends HecatonTestSupport {

    private final File parent = new File("ports");
    private final File child = new File(parent, "hc.ports");

    @Before
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void setUp() {
        parent.mkdir();
    }

    @After
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void tearDown() {
        child.delete();
        parent.delete();

        Hecaton.shutdownAll();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(HecatonMemberStarter.class);
    }

    @Test
    public void testMain() throws Exception {
        System.setProperty("print.port", child.getName());

        HecatonMemberStarter.main(new String[]{});

        assertEquals(1, Hecaton.getAllHecatonInstances().size());
        assertTrue(child.exists());
    }
}
