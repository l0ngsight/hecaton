/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.spring.autoconfigure;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.client.config.YamlClientConfigBuilder;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.config.YamlConfigBuilder;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spring.context.SpringManagedContext;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * What the Spring Boot auto-configuration of {@code hecaton-spring-boot3} and
 * {@code hecaton-spring-boot4} does, without depending on a Spring Boot version (spec §5.2).
 * <p>
 * An application gets a {@link HecatonInstance} bean when it declares no instance itself and
 * one of these is present, in this order:
 * <ol>
 *     <li>a {@link Config} bean: a member;</li>
 *     <li>member configuration: the {@value #CONFIG_PROPERTY} property pointing to a member
 *     configuration file, the {@code hecaton.config} system property, or
 *     {@code hecaton.xml}/{@code .yaml}/{@code .yml} in the working directory or on the
 *     classpath: a member;</li>
 *     <li>a {@link ClientConfig} bean: a client;</li>
 *     <li>client configuration: {@value #CONFIG_PROPERTY} pointing to a client configuration
 *     file, the {@code hecaton.client.config} system property, or
 *     {@code hecaton-client.xml}/{@code .yaml}/{@code .yml}: a client.</li>
 * </ol>
 * Without any of these no instance is created, as with Spring Boot's own Hazelcast support.
 * The instance uses the application's class loader and, unless the configuration sets one,
 * a {@link SpringManagedContext}, so that {@code @SpringAware} objects get their dependencies.
 */
public final class HecatonAutoConfigurationSupport {

    /** The Spring property that names the configuration file (a Spring resource location). */
    public static final String CONFIG_PROPERTY = "spring.hecaton.config";

    private static final String MEMBER_CONFIG_SYSTEM_PROPERTY = "hecaton.config";
    private static final String CLIENT_CONFIG_SYSTEM_PROPERTY = "hecaton.client.config";
    private static final String[] MEMBER_CONFIG_FILES = {"hecaton.xml", "hecaton.yaml", "hecaton.yml"};
    private static final String[] CLIENT_CONFIG_FILES = {"hecaton-client.xml", "hecaton-client.yaml", "hecaton-client.yml"};
    private static final String[] DEFAULT_LOCATIONS = {"file:./", "classpath:/"};

    private static final Pattern XML_CLIENT_ROOT = Pattern.compile("<(?:[\\w-]+:)?hecaton-client[\\s>]");
    private static final Pattern XML_FAILOVER_ROOT = Pattern.compile("<(?:[\\w-]+:)?hecaton-client-failover[\\s>]");
    private static final Pattern YAML_CLIENT_ROOT = Pattern.compile("(?m)^hecaton-client:");
    private static final Pattern YAML_FAILOVER_ROOT = Pattern.compile("(?m)^hecaton-client-failover:");

    private HecatonAutoConfigurationSupport() {
    }

    /** The configuration file named by {@value #CONFIG_PROPERTY}, or {@code null}. */
    @Nullable
    static Resource configuredResource(Environment environment, ResourceLoader resourceLoader) {
        String location = environment.getProperty(CONFIG_PROPERTY);
        if (location == null || location.isBlank()) {
            return null;
        }
        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            throw new InvalidConfigurationException(CONFIG_PROPERTY + " names a file that does not exist: " + location);
        }
        return resource;
    }

    /** Whether there is member configuration (a {@code Config} bean is checked separately). */
    public static boolean hasMemberConfig(Environment environment, ResourceLoader resourceLoader) {
        Resource configured = configuredResource(environment, resourceLoader);
        if (configured != null) {
            return !isClientConfig(configured);
        }
        return System.getProperty(MEMBER_CONFIG_SYSTEM_PROPERTY) != null
                || findDefault(MEMBER_CONFIG_FILES, resourceLoader) != null;
    }

    /** Whether there is client configuration (a {@code ClientConfig} bean is checked separately). */
    public static boolean hasClientConfig(Environment environment, ResourceLoader resourceLoader) {
        Resource configured = configuredResource(environment, resourceLoader);
        if (configured != null) {
            return isClientConfig(configured);
        }
        return System.getProperty(CLIENT_CONFIG_SYSTEM_PROPERTY) != null
                || findDefault(CLIENT_CONFIG_FILES, resourceLoader) != null;
    }

    /** Starts a member with the {@code config} bean, or with the member configuration found. */
    public static HecatonInstance newMember(@Nullable Config config, ApplicationContext context) {
        Config memberConfig = config != null ? config : loadMemberConfig(context.getEnvironment(), context);
        if (memberConfig.getClassLoader() == null) {
            memberConfig.setClassLoader(context.getClassLoader());
        }
        if (memberConfig.getManagedContext() == null) {
            memberConfig.setManagedContext(new SpringManagedContext(context));
        }
        return memberConfig.getInstanceName() != null
                ? Hecaton.getOrCreateHecatonInstance(memberConfig)
                : Hecaton.newHecatonInstance(memberConfig);
    }

    /** Starts a client with the {@code config} bean, or with the client configuration found. */
    public static HecatonInstance newClient(@Nullable ClientConfig config, ApplicationContext context) {
        ClientConfig clientConfig = config != null ? config : loadClientConfig(context.getEnvironment(), context);
        if (clientConfig.getClassLoader() == null) {
            clientConfig.setClassLoader(context.getClassLoader());
        }
        if (clientConfig.getManagedContext() == null) {
            clientConfig.setManagedContext(new SpringManagedContext(context));
        }
        return HecatonClient.newHecatonClient(clientConfig);
    }

    static Config loadMemberConfig(Environment environment, ResourceLoader resourceLoader) {
        Resource resource = configuredResource(environment, resourceLoader);
        if (resource == null && System.getProperty(MEMBER_CONFIG_SYSTEM_PROPERTY) != null) {
            return Config.load();
        }
        if (resource == null) {
            resource = findDefault(MEMBER_CONFIG_FILES, resourceLoader);
        }
        if (resource == null) {
            throw new InvalidConfigurationException("No Hecaton member configuration found");
        }
        try (InputStream in = resource.getInputStream()) {
            return isYaml(resource) ? new YamlConfigBuilder(in).build() : new XmlConfigBuilder(in).build();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + resource, e);
        }
    }

    static ClientConfig loadClientConfig(Environment environment, ResourceLoader resourceLoader) {
        Resource resource = configuredResource(environment, resourceLoader);
        if (resource == null && System.getProperty(CLIENT_CONFIG_SYSTEM_PROPERTY) != null) {
            return ClientConfig.load();
        }
        if (resource == null) {
            resource = findDefault(CLIENT_CONFIG_FILES, resourceLoader);
        }
        if (resource == null) {
            throw new InvalidConfigurationException("No Hecaton client configuration found");
        }
        try (InputStream in = resource.getInputStream()) {
            return isYaml(resource) ? new YamlClientConfigBuilder(in).build() : new XmlClientConfigBuilder(in).build();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + resource, e);
        }
    }

    /** Whether {@code resource} holds client configuration, judged by its root element or key. */
    static boolean isClientConfig(Resource resource) {
        String text;
        try (InputStream in = resource.getInputStream()) {
            text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + resource, e);
        }
        boolean yaml = isYaml(resource);
        if ((yaml ? YAML_FAILOVER_ROOT : XML_FAILOVER_ROOT).matcher(text).find()) {
            throw new InvalidConfigurationException(CONFIG_PROPERTY + " names a failover client configuration, which "
                    + "the auto-configuration does not support; declare a HecatonInstance bean created with "
                    + "HecatonClient.newHecatonFailoverClient instead: " + resource);
        }
        return (yaml ? YAML_CLIENT_ROOT : XML_CLIENT_ROOT).matcher(text).find();
    }

    @Nullable
    private static Resource findDefault(String[] fileNames, ResourceLoader resourceLoader) {
        for (String location : DEFAULT_LOCATIONS) {
            for (String fileName : fileNames) {
                Resource resource = resourceLoader.getResource(location + fileName);
                if (resource.exists()) {
                    return resource;
                }
            }
        }
        return null;
    }

    private static boolean isYaml(Resource resource) {
        String name = resource.getFilename() == null ? "" : resource.getFilename().toLowerCase(Locale.ROOT);
        return name.endsWith(".yaml") || name.endsWith(".yml");
    }
}
