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

package uk.org.hecaton.client.config;

import uk.org.hecaton.client.config.impl.ClientConfigSections;
import uk.org.hecaton.client.config.impl.YamlClientConfigLocator;
import uk.org.hecaton.client.config.impl.YamlClientDomConfigProcessor;
import uk.org.hecaton.config.AbstractYamlConfigBuilder;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.internal.config.YamlConfigSchemaValidator;
import uk.org.hecaton.internal.config.ConfigLoader;
import uk.org.hecaton.internal.config.yaml.YamlDomChecker;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.internal.yaml.YamlLoader;
import uk.org.hecaton.internal.yaml.YamlMapping;
import uk.org.hecaton.internal.yaml.YamlNode;
import uk.org.hecaton.spi.annotation.PrivateApi;
import org.w3c.dom.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Properties;

import static uk.org.hecaton.internal.config.yaml.W3cDomUtil.asW3cNode;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.Preconditions.checkTrue;

/**
 * Loads the {@link uk.org.hecaton.client.config.ClientConfig} using YAML.
 */
public class YamlClientConfigBuilder extends AbstractYamlConfigBuilder {

    private final InputStream in;

    public YamlClientConfigBuilder(String resource) throws IOException {
        URL url = ConfigLoader.locateConfig(resource);
        checkTrue(url != null, "Could not load " + resource);
        this.in = url.openStream();
    }

    public YamlClientConfigBuilder(File file) throws IOException {
        checkNotNull(file, "File is null!");
        this.in = new FileInputStream(file);
    }

    public YamlClientConfigBuilder(URL url) throws IOException {
        checkNotNull(url, "URL is null!");
        this.in = url.openStream();
    }

    public YamlClientConfigBuilder(InputStream in) {
        this.in = in;
    }

    /**
     * Loads the client config using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hecaton.client.config' is set. If it exist and
     * it begins with 'classpath:', then a classpath resource is loaded. Else it will assume it is a file
     * reference. The configuration file or resource will be loaded only if the postfix of its name ends
     * with `.yaml`.</li>
     * <li>it checks if a hecaton-client.yaml is available in the working dir</li>
     * <li>it checks if a hecaton-client.yaml is available on the classpath</li>
     * <li>it loads the hecaton-client-default.yaml</li>
     * </ol>
     */
    public YamlClientConfigBuilder() {
        this((YamlClientConfigLocator) null);
    }

    /**
     * Constructs a {@link YamlClientConfigBuilder} that loads the configuration
     * with the provided {@link YamlClientConfigLocator}.
     * <p>
     * If the provided {@link YamlClientConfigLocator} is {@code null}, a new
     * instance is created and the config is located in every possible
     * places. For these places, please see {@link YamlClientConfigLocator}.
     * <p>
     * If the provided {@link YamlClientConfigLocator} is not {@code null}, it
     * is expected that it already located the configuration YAML to load
     * from. No further attempt to locate the configuration YAML is made
     * if the configuration YAML is not located already.
     *
     * @param locator the configured locator to use
     */
    @PrivateApi
    public YamlClientConfigBuilder(YamlClientConfigLocator locator) {
        if (locator == null) {
            locator = new YamlClientConfigLocator();
            locator.locateEverywhere();
        }

        this.in = locator.getIn();
    }

    public ClientConfig build() {
        return build(Thread.currentThread().getContextClassLoader());
    }

    public ClientConfig build(ClassLoader classLoader) {
        ClientConfig clientConfig = new ClientConfig();
        build(clientConfig, classLoader);
        return clientConfig;
    }

    public YamlClientConfigBuilder setProperties(Properties properties) {
        setPropertiesInternal(properties);
        return this;
    }

    void build(ClientConfig clientConfig, ClassLoader classLoader) {
        clientConfig.setClassLoader(classLoader);
        try {
            parseAndBuildConfig(clientConfig);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private void parseAndBuildConfig(ClientConfig config) throws Exception {
        YamlMapping yamlRootNode;
        try {
            yamlRootNode = ((YamlMapping) YamlLoader.load(in));
        } catch (Exception ex) {
            throw new InvalidConfigurationException("Invalid YAML configuration", ex);
        }

        YamlNode clientRoot = yamlRootNode.childAsMapping(ClientConfigSections.HECATON_CLIENT.getName());
        if (clientRoot == null) {
            clientRoot = yamlRootNode;
        }
        YamlDomChecker.check(clientRoot, Collections.singleton(ClientConfigSections.HECATON_CLIENT.getName()));

        Node w3cRootNode = asW3cNode(clientRoot);
        replaceVariables(w3cRootNode);
        importDocuments(clientRoot);

        if (shouldValidateTheSchema()) {
            new YamlConfigSchemaValidator().validate(yamlRootNode);
        }

        new YamlClientDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }

    @Override
    protected String getConfigRoot() {
        return ClientConfigSections.HECATON_CLIENT.getName();
    }
}
