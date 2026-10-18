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

package uk.org.hecaton.osgi.impl;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.osgi.TestBundle;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public abstract class HecatonOSGiScriptingTest extends HecatonTestSupport {

    protected TestBundle bundle;

    @Rule
    public TestRule scriptingAvailableRule = new TestRule() {

        @Override
        public Statement apply(Statement base, Description description) {
            if (Activator.isJavaxScriptingAvailable()) {
                return base;
            } else {
                return new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        System.out.println("Ignoring test since scripting is not available!");
                    }
                };
            }
        }

    };

    @Before
    public void setup() throws Exception {
        bundle = new TestBundle();
        bundle.start();
    }

    @After
    public void tearDown() throws Exception {
        try {
            bundle.stop();
            bundle = null;
        } finally {
            Hecaton.shutdownAll();
        }
    }
}
