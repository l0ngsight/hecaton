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

package uk.org.hecaton.client.config.impl;

import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.internal.config.AbstractConfigLocator;

import static uk.org.hecaton.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_CONFIG;
import static uk.org.hecaton.internal.config.DeclarativeConfigUtil.XML_ACCEPTED_SUFFIXES;

/**
 * A support class for the {@link XmlClientConfigBuilder} to locate the client
 * xml configuration.
 */
public class XmlClientConfigLocator extends AbstractConfigLocator {

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(SYSPROP_CLIENT_CONFIG, XML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateFromSystemPropertyOrFailOnUnacceptedSuffix() {
        return loadFromSystemPropertyOrFailOnUnacceptedSuffix(SYSPROP_CLIENT_CONFIG, XML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory("hecaton-client.xml");
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath("hecaton-client.xml");
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath("hecaton-client-default.xml");
        return true;
    }
}
