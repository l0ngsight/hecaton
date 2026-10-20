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

package uk.org.hecaton.logging;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.impl.LoggingServiceImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.IsolatedLoggingRule;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;

import static uk.org.hecaton.test.IsolatedLoggingRule.LOGGING_TYPE_NONE;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class NoLoggerLevelChangeTest extends HecatonTestSupport {

    @Rule
    public final IsolatedLoggingRule isolatedLoggingRule = new IsolatedLoggingRule();

    private LoggingServiceImpl loggingService;

    @Before
    public void before() {
        isolatedLoggingRule.setLoggingType(LOGGING_TYPE_NONE);
        HecatonInstance instance = createHecatonInstance();
        loggingService = (LoggingServiceImpl) instance.getLoggingService();
    }

    @Test
    public void test() {
        // no error should be reported

        loggingService.setLevel(Level.FINEST);
        loggingService.resetLevel();
    }

}
