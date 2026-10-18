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

package uk.org.hecaton.config;

import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.spi.properties.HecatonProperty;

import static uk.org.hecaton.instance.BuildInfoProvider.HECATON_INTERNAL_OVERRIDE_VERSION;

public class AbstractConfigBuilder {

    /**
     * Set it to {@code false} to skip the configuration validation.
     */
    private static final HecatonProperty VALIDATION_ENABLED_PROP
            = new HecatonProperty("hecaton.config.schema.validation.enabled", "true");

    protected final boolean shouldValidateTheSchema() {
        return new HecatonProperties(System.getProperties()).getBoolean(VALIDATION_ENABLED_PROP)
                && System.getProperty(HECATON_INTERNAL_OVERRIDE_VERSION) == null;
    }

}
