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

package uk.org.hecaton.osgi;

import uk.org.hecaton.config.Config;
import org.osgi.framework.Bundle;

import java.util.Set;

/**
 * Contract point for Hecaton services on top of OSGI.
 */
public interface HecatonOSGiService {

    /**
     * System property for starting a default Hecaton instance per Hecaton OSGI bundle.
     */
    String HECATON_OSGI_START = "hecaton.osgi.start";

    /**
     * System property for disabling the behaviour that registers created Hecaton instances as OSGI service.
     */
    String HECATON_OSGI_REGISTER_DISABLED = "hecaton.osgi.register.disabled";

    /**
     * System property for disabling the behaviour that gives different cluster name
     * to each Hecaton bundle.
     */
    String HECATON_OSGI_GROUPING_DISABLED = "hecaton.osgi.grouping.disabled";

    /**
     * System property for disabling the JSR-223 script engine scan by the Hecaton OSGI service.
     */
    String HECATON_OSGI_JSR223_DISABLED = "hecaton.osgi.jsr223.disabled";

    /**
     * Gets the ID of service.
     *
     * @return the ID of service
     */
    String getId();

    /**
     * Gets the owner {@link Bundle} of this instance.
     *
     * @return the owner {@link Bundle} of this instance
     */
    Bundle getOwnerBundle();

    /**
     * Gets the default {@link HecatonOSGiInstance}.
     *
     * @return the default {@link HecatonOSGiInstance}
     */
    HecatonOSGiInstance getDefaultHecatonInstance();

    /**
     * Creates a new {@link HecatonOSGiInstance}
     * on the owner bundle with specified configuration.
     *
     * @param config Configuration for the new {@link HecatonOSGiInstance} (member)
     * @return the new {@link HecatonOSGiInstance}
     */
    HecatonOSGiInstance newHecatonInstance(Config config);

    /**
     * Creates a new {@link HecatonOSGiInstance}
     * on the owner bundle with default configuration.
     *
     * @return the new {@link HecatonOSGiInstance}
     */
    HecatonOSGiInstance newHecatonInstance();

    /**
     * Gets an existing {@link HecatonOSGiInstance} with its <code>instanceName</code>.
     *
     * @param instanceName Name of the {@link HecatonOSGiInstance} (member)
     * @return an existing {@link HecatonOSGiInstance}
     */
    HecatonOSGiInstance getHecatonInstanceByName(String instanceName);

    /**
     * Gets all active/running {@link HecatonOSGiInstance}s on the owner bundle.
     *
     * @return all active/running {@link HecatonOSGiInstance}s on the owner bundle
     */
    Set<HecatonOSGiInstance> getAllHecatonInstances();

    /**
     * Shuts down the given {@link HecatonOSGiInstance} on the owner bundle.
     *
     * @param instance the {@link HecatonOSGiInstance} to shut down
     */
    void shutdownHecatonInstance(HecatonOSGiInstance instance);

    /**
     * Shuts down all running {@link HecatonOSGiInstance}s on the owner bundle.
     */
    void shutdownAll();

}
