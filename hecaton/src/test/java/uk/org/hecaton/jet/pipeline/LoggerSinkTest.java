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

package uk.org.hecaton.jet.pipeline;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LoggerSinkTest extends JetTestSupport {

    private static final String HECATON_LOGGING_TYPE = "hecaton.logging.type";
    private static final String HECATON_LOGGING_CLASS = "hecaton.logging.class";

    private String prevLoggingType;
    private String prevLoggingClass;

    @Before
    public void setup() {
        prevLoggingType = System.getProperty(HECATON_LOGGING_TYPE);
        prevLoggingClass = System.getProperty(HECATON_LOGGING_CLASS);

        System.clearProperty(HECATON_LOGGING_TYPE);
        System.setProperty(HECATON_LOGGING_CLASS, MockLoggingFactory.class.getCanonicalName());
    }

    @Test
    public void loggerSink() {
        // Given
        HecatonInstance hz = createHecatonInstance();
        String srcName = randomName();

        hz.getList(srcName).add(0);

        Pipeline p = Pipeline.create();

        // When
        p.readFrom(Sources.<Integer>list(srcName))
         .map(i -> i + "-shouldBeSeenOnTheSystemOutput")
         .writeTo(Sinks.logger());

        hz.getJet().newJob(p).join();

        // Then
        Assert.assertTrue("no message containing '0-shouldBeSeenOnTheSystemOutput' was found",
                MockLoggingFactory.capturedMessages
                        .stream()
                        .anyMatch(message -> message.contains("0-shouldBeSeenOnTheSystemOutput"))
        );
    }

    @After
    public void after() {
        if (prevLoggingType == null) {
            System.clearProperty(HECATON_LOGGING_TYPE);
        } else {
            System.setProperty(HECATON_LOGGING_TYPE, prevLoggingType);
        }
        if (prevLoggingClass == null) {
            System.clearProperty(HECATON_LOGGING_CLASS);
        } else {
            System.setProperty(HECATON_LOGGING_CLASS, prevLoggingClass);
        }
    }

}
