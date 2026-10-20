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

package uk.org.hecaton.internal.util;

import uk.org.hecaton.internal.util.ModularJavaUtils.PackageAccessRequirement;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static uk.org.hecaton.internal.util.ModularJavaUtils.PackageAccessRequirement.open;
import static uk.org.hecaton.internal.util.ModularJavaUtils.PackageAccessRequirement.packages;
import static uk.org.hecaton.internal.util.ModularJavaUtils.checkJavaInternalAccess;
import static uk.org.hecaton.internal.util.ModularJavaUtils.checkPackageRequirements;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests that {@link ModularJavaUtils} correctly logs warnings about missing package access on Java 9+.
 */
@Category({ QuickTest.class, ParallelJVMTest.class })
public class ModularJavaUtilsTest {

    /**
     * Tests that the testsuite is running without missing-package-access warnings. I.e. the access to Java internal packages is
     * provided.
     */
    @Test
    public void testNoMissingPackageAccessInTestsuite() {
        ILogger logger = mock(ILogger.class);

        checkJavaInternalAccess(logger);

        verify(logger, never()).warning(anyString());
    }

    /**
     * Tests that {@link ModularJavaUtils#checkPackageRequirements(ILogger, Map)} logs a warning with the missing
     * Java argument if a Java internal package access not provided to Hecaton and the test is running on Java 9 or newer.
     */
    @Test
    public void testMissingAccess() {
        ILogger logger = mock(ILogger.class);

        Map<String, PackageAccessRequirement[]> requirements = new HashMap<>();
        requirements.put("java.base", packages(open("jdk.internal.misc")));
        checkPackageRequirements(logger, requirements);

        verify(logger, times(1)).warning(contains("--add-opens java.base/jdk.internal.misc=ALL-UNNAMED"));
    }

    @Test
    public void testHecatonModuleName() {
        assertNull("Hecaton module name should be null as the testsuite runs hecaton on the classpath",
                ModularJavaUtils.getHecatonModuleName());
    }

    @Test
    public void testNoExceptionWhenNullLogger() {
        checkJavaInternalAccess(null);
    }

}
