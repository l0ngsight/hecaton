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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Log4j2Factory;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.IsolatedLoggingRule;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.IsolatedLoggingRule.LOGGING_TYPE_JDK;
import static uk.org.hecaton.test.IsolatedLoggingRule.LOGGING_TYPE_LOG4J2;
import static uk.org.hecaton.test.IsolatedLoggingRule.LOGGING_TYPE_PROPERTY;
import static org.junit.Assert.assertSame;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientLoggerConfigurationTest extends HecatonTestSupport {

    @Rule
    public final IsolatedLoggingRule isolatedLoggingRule = new IsolatedLoggingRule();

    private TestHecatonFactory hecatonFactory;

    @After
    public void tearDown() {
        hecatonFactory.shutdownAll();
    }

    @Test
    public void testProgrammaticConfiguration() {
        testLoggingWithConfiguration(true);
    }

    @Test
    public void testSystemPropertyConfiguration() {
        testLoggingWithConfiguration(false);
    }

    // Test with programmatic or system property configuration according to boolean parameter.

    // the idea of the test is to configure a specific logging type for a client and then
    // test its LoggingService produce instances of the expected Logger impl
    protected void testLoggingWithConfiguration(boolean programmaticConfiguration) {
        hecatonFactory = new TestHecatonFactory();
        Config cg = new Config();
        cg.setProperty(LOGGING_TYPE_PROPERTY, LOGGING_TYPE_JDK);
        hecatonFactory.newHecatonInstance(cg);

        ClientConfig config = new ClientConfig();
        if (programmaticConfiguration) {
            config.setProperty(LOGGING_TYPE_PROPERTY, LOGGING_TYPE_LOG4J2);
        } else {
            isolatedLoggingRule.setLoggingType(LOGGING_TYPE_LOG4J2);
        }
        HecatonInstance client = hecatonFactory.newHecatonClient(config);

        ILogger clientLogger = client.getLoggingService().getLogger("loggerName");
        // this part is fragile.
        // client wraps the actual logger in its own class
        ILogger actualLogger = getFieldValue(clientLogger, "logger");
        Class<?> clientLoggerClass = actualLogger.getClass();

        ILogger expectedLogger = new Log4j2Factory().getLogger("expectedLogger");
        Class<?> expectedLoggerClass = expectedLogger.getClass();

        assertSame(expectedLoggerClass, clientLoggerClass);
    }
}
