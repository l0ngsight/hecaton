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

package uk.org.hecaton.jet.hadoop.impl;

import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.test.IgnoreInJenkinsOnWindows;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({QuickTest.class, ParallelJVMTest.class, IgnoreInJenkinsOnWindows.class})
@RunWith(HecatonSerialClassRunner.class)
public abstract class HadoopTestSupport extends SimpleTestInClusterSupport {

    @Before
    public void hadoopSupportBefore() {
        if (useHadoop()) {
            // Tests fail on windows without an extra setup. If you want to run them, comment out this
            // line and follow instructions here: https://stackoverflow.com/a/35652866/952135
            assumeThatNoWindowsOS();

            // Tests might fail on some IBM JDKs with error:
            // No LoginModule found for com.ibm.security.auth.module.JAASLoginModule
            // see https://github.com/hazelcast/hazelcast/issues/20754
            assumeHadoopSupportsIbmPlatform();
        }
    }

    protected boolean useHadoop() {
        return true;
    }
}
