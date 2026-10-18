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

package uk.org.hecaton.dataconnection.impl.hecatondataconnection;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.internal.util.StringUtil;
import uk.org.hecaton.jet.impl.util.ImdgUtil;

import static uk.org.hecaton.dataconnection.HecatonDataConnection.CLIENT_XML;
import static uk.org.hecaton.dataconnection.HecatonDataConnection.CLIENT_YML;

public class HecatonDataConnectionClientConfigBuilder {

    /**
     * Converts the string in the DataConnectionConfig to ClientConfig object
     */
    public ClientConfig buildClientConfig(DataConnectionConfig dataConnectionConfig) {

        String clientXml = dataConnectionConfig.getProperty(CLIENT_XML);
        if (!StringUtil.isNullOrEmpty(clientXml)) {
            // Read ClientConfig from XML
            return ImdgUtil.asClientConfig(clientXml);
        }

        String clientYaml = dataConnectionConfig.getProperty(CLIENT_YML);
        if (!StringUtil.isNullOrEmpty(clientYaml)) {
            // Read ClientConfig from Yaml
            return ImdgUtil.asClientConfigFromYaml(clientYaml);
        }
        throw new HecatonException("Invalid state. Both XML and YML strings are empty. HecatonDataConnection with name '"
                                     + dataConnectionConfig.getName()
                                     + "' could not be created");
    }
}
