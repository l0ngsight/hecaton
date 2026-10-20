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

package uk.org.hecaton.instance;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.server.HecatonMemberStarter;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;

import static uk.org.hecaton.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;

/**
 * Separate test because it cannot run in parallel with any other instance
 * tracking tests.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class InstanceTrackingServerModeTest extends HecatonTestSupport {

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public final OverridePropertyRule configOverrideRule
            = set("hecaton.config", "classpath:hecaton-instance-tracking-test.xml");

    @After
    public void tearDown() {
        System.clearProperty("instance_tracking_filename");
        Hecaton.shutdownAll();
    }

    @Test
    public void testServerMode() throws Exception {
        File tempFile = tempFolder.newFile();
        System.setProperty("instance_tracking_filename", tempFile.getAbsolutePath());

        HecatonMemberStarter.main(new String[]{});

        String actualContents = Files.readString(tempFile.toPath());
        JsonObject json = Json.parse(actualContents).asObject();
        assertEquals("server", json.getString("mode", ""));
    }

}
