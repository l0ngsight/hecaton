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

package uk.org.hecaton.client.impl.clientside;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.LogEvent;
import uk.org.hecaton.logging.LogListener;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;

import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientLoggingServiceTest extends HecatonTestSupport {

    private TestHecatonFactory factory;
    private LoggingService loggingService;
    private LogListener logListener;
    private LogEvent logEvent;

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();

        HecatonInstance member = factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient();

        loggingService = client.getLoggingService();

        logListener = new LogListener() {
            @Override
            public void log(LogEvent logEvent) {
            }
        };

        logEvent = new LogEvent(null, member.getCluster().getLocalMember());
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLogListener() {
        loggingService.addLogListener(Level.INFO, logListener);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveLogListener() {
        loggingService.removeLogListener(logListener);
    }

    @Test
    public void testLog_whenLogEvent_thenNothingHappens() {
        ILogger logger = loggingService.getLogger("test");
        logger.log(logEvent);
    }

    @Test
    public void testLog_whenGetLevel_thenDefaultLevelIsReturned() {
        ILogger logger = loggingService.getLogger("test");
        assertNotNull(logger.getLevel());
    }
}
