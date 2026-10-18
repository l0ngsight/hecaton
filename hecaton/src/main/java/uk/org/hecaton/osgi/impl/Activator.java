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

import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.osgi.HecatonOSGiService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.lang.reflect.Method;

/**
 * Hecaton OSGi bundle activator.
 *
 * @see org.osgi.framework.BundleActivator
 */
public class Activator
        implements BundleActivator {

    private static final ILogger LOGGER = Logger.getLogger(Activator.class);

    // Defined as `volatile` since Javadoc of `BundleActivator` says that
    // `The Framework must not concurrently call a BundleActivator object`
    // and also it is marked as `@NotThreadSafe`.
    // So it can be called from different threads but cannot be called concurrently.
    private volatile HecatonInternalOSGiService hecatonOSGiService;

    @Override
    public void start(BundleContext context)
            throws Exception {
        // Try to start javax.scripting - JSR 223
        activateJavaxScripting(context);

        assert hecatonOSGiService == null : "Hecaton OSGI service should be null while starting!";

        // Create a new Hecaton OSGI service with given bundle and its context
        hecatonOSGiService = new HecatonOSGiServiceImpl(context.getBundle());

        // Activate new created Hecaton OSGI service
        hecatonOSGiService.activate();
    }

    @Override
    public void stop(BundleContext context)
            throws Exception {
        assert hecatonOSGiService != null : "Hecaton OSGI service should not be null while stopping!";

        hecatonOSGiService.deactivate();
        hecatonOSGiService = null;
    }

    private void activateJavaxScripting(BundleContext context)
            throws Exception {
        if (isJavaxScriptingAvailable()) {
            Class<?> clazz = context.getBundle().loadClass("uk.org.hecaton.osgi.impl.ScriptEngineActivator");
            Method register = clazz.getDeclaredMethod("registerOsgiScriptEngineManager", BundleContext.class);
            register.setAccessible(true);
            register.invoke(clazz, context);
        } else {
            LOGGER.warning("javax.scripting is not available, scripts from Management Center cannot be executed!");
        }
    }

    static boolean isJavaxScriptingAvailable() {
        if (Boolean.getBoolean(HecatonOSGiService.HECATON_OSGI_JSR223_DISABLED)) {
            return false;
        }
        try {
            Class.forName("javax.script.ScriptEngineManager");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

}
