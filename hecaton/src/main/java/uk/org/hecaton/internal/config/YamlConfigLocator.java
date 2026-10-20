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

package uk.org.hecaton.internal.config;

import static uk.org.hecaton.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static uk.org.hecaton.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES;

/**
 * Support class for the {@link uk.org.hecaton.config.YamlConfigBuilder} that locates the YAML configuration:
 * <ol>
 * <li>system property</li>
 * <li>working directory</li>
 * <li>classpath</li>
 * <li>default</li>
 * </ol>
 */
public class YamlConfigLocator extends AbstractConfigLocator {

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(SYSPROP_MEMBER_CONFIG, YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateFromSystemPropertyOrFailOnUnacceptedSuffix() {
        return loadFromSystemPropertyOrFailOnUnacceptedSuffix(SYSPROP_MEMBER_CONFIG, YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory("hecaton", YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath("hecaton", YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath("hecaton-default.yaml");
        return true;
    }
}
