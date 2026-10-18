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

package uk.org.hecaton.internal.usercodedeployment;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.UserCodeDeploymentConfig;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.usercodedeployment.impl.ClassData;
import uk.org.hecaton.internal.usercodedeployment.impl.ClassDataProvider;
import uk.org.hecaton.internal.usercodedeployment.impl.ClassLocator;
import uk.org.hecaton.internal.usercodedeployment.impl.ClassSource;
import uk.org.hecaton.internal.util.filter.Filter;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.internal.usercodedeployment.impl.filter.ClassNameFilterParser.parseClassNameFilters;
import static uk.org.hecaton.internal.usercodedeployment.impl.filter.MemberProviderFilterParser.parseMemberFilter;
import static uk.org.hecaton.jet.impl.util.Util.CONFIG_CHANGE_TEMPLATE;
import static java.lang.String.format;

/**
 * @deprecated "User Code Deployment" is replaced by the "User Code Namespaces" feature
 * @see UserCodeNamespaceService
 */
@Deprecated(since = "5.4", forRemoval = true)
public final class UserCodeDeploymentService implements ManagedService {

    public static final String SERVICE_NAME = "user-code-deployment-service";

    private volatile boolean enabled;

    private ClassDataProvider provider;
    private ClassLocator locator;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        UserCodeDeploymentConfig config = nodeEngine.getConfig().getUserCodeDeploymentConfig();
        if (!config.isEnabled()) {
            return;
        }

        ClassLoader parent = nodeEngine.getConfigClassLoader().getParent();
        Filter<String> classNameFilter = parseClassNameFilters(config);
        Filter<Member> memberFilter = parseMemberFilter(config.getProviderFilter());
        ConcurrentMap<String, ClassSource> classMap = new ConcurrentHashMap<>();
        ConcurrentMap<String, ClassSource> clientClassMap = new ConcurrentHashMap<>();

        UserCodeDeploymentConfig.ProviderMode providerMode = config.getProviderMode();
        ILogger providerLogger = nodeEngine.getLogger(ClassDataProvider.class);
        provider = new ClassDataProvider(providerMode, parent, classMap, clientClassMap, providerLogger);

        UserCodeDeploymentConfig.ClassCacheMode classCacheMode = config.getClassCacheMode();
        locator = new ClassLocator(classMap, clientClassMap, parent, classNameFilter, memberFilter, classCacheMode, nodeEngine);
        enabled = config.isEnabled();
    }

    public void defineClasses(List<Map.Entry<String, byte[]>> classDefinitions) {
        if (!enabled) {
            throw new IllegalStateException("User Code Deployment is not enabled. "
                    + "To enable User Code Deployment, do one of the following:\n"
                    + format(CONFIG_CHANGE_TEMPLATE,
                        "config.getUserCodeDeploymentConfig().setEnabled(true);",
                        "hecaton.user-code-deployment.enabled to true",
                        "-Dhc.user-code-deployment.enabled=true",
                        "HC_USERCODEDEPLOYMENT_ENABLED=true"));
        }
        locator.defineClassesFromClient(classDefinitions);
    }

    // called by operations sent by other members
    public ClassData getClassDataOrNull(String className) {
        if (!enabled) {
            return null;
        }
        return provider.getClassDataOrNull(className);
    }

    // called by User Code Deployment classloader on this member
    public Class<?> handleClassNotFoundException(String name)
            throws ClassNotFoundException {
        if (!enabled) {
            throw new ClassNotFoundException("User Code Deployment is not enabled. Cannot find class " + name);
        }
        return locator.handleClassNotFoundException(name);
    }

    // called by User Code Deployment classloader on this member
    public Class<?> findLoadedClass(String name) {
        if (!enabled) {
            return null;
        }
        return locator.findLoadedClass(name);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }
}
