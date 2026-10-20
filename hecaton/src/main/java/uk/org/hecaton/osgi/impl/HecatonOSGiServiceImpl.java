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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.osgi.HecatonOSGiInstance;
import uk.org.hecaton.osgi.HecatonOSGiService;
import uk.org.hecaton.internal.util.ExceptionUtil;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmpty;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implementation of {@link HecatonInternalOSGiService}.
 *
 * @see HecatonInternalOSGiService
 * @see uk.org.hecaton.osgi.HecatonOSGiService
 */
class HecatonOSGiServiceImpl
        implements HecatonInternalOSGiService {

    private static final ILogger LOGGER = Logger.getLogger(HecatonOSGiService.class);

    private final Object serviceMutex = new Object();

    private final Bundle ownerBundle;
    private final BundleContext ownerBundleContext;
    private final String id;
    private final ConcurrentMap<HecatonOSGiInstance, ServiceRegistration> instanceServiceRegistrationMap
            = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, HecatonOSGiInstance> instanceMap
            = new ConcurrentHashMap<>();

    // No need to "volatile" since this field is guarded and happens-before is handled by
    // `synchronized` blocks of `serviceMutex` instance.
    private ServiceRegistration serviceRegistration;

    // Defined as `volatile` because even though this field is written inside `synchronized` blocks,
    // it can be read from any thread without `synchronized` blocks and without `volatile` they may seen invalid value.
    private volatile HecatonOSGiInstance hecatonInstance;

    HecatonOSGiServiceImpl(Bundle ownerBundle) {
        this(ownerBundle, DEFAULT_ID);
    }

    HecatonOSGiServiceImpl(Bundle ownerBundle, String id) {
        this.ownerBundle = ownerBundle;
        this.ownerBundleContext = ownerBundle.getBundleContext();
        this.id = id;
    }

    private void checkActive() {
        if (!isActive()) {
            throw new IllegalStateException("Hecaton OSGI Service is not active!");
        }
    }

    private boolean shouldSetClusterName(String clusterName) {
        return (isNullOrEmpty(clusterName) || Config.DEFAULT_CLUSTER_NAME.equals(clusterName))
                && !Boolean.getBoolean(HECATON_OSGI_GROUPING_DISABLED);
    }

    private Config getConfig(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        if (shouldSetClusterName(config.getClusterName())) {
            config.setClusterName(id);
        }
        return config;
    }

    private HecatonInstance createHecatonInstance(Config config) {
        return Hecaton.newHecatonInstance(getConfig(config));
    }

    private HecatonOSGiInstance registerInstance(HecatonInstance instance) {
        HecatonOSGiInstance hecatonOSGiInstance;
        if (instance instanceof HecatonOSGiInstance osgiInstance) {
            hecatonOSGiInstance = osgiInstance;
        } else {
            hecatonOSGiInstance = new HecatonOSGiInstanceImpl(instance, this);
        }
        if (!Boolean.getBoolean(HECATON_OSGI_REGISTER_DISABLED)) {
            ServiceRegistration serviceRegistration =
                    ownerBundleContext.registerService(HecatonInstance.class.getName(), hecatonOSGiInstance, null);
            instanceServiceRegistrationMap.put(hecatonOSGiInstance, serviceRegistration);
        }
        instanceMap.put(instance.getName(), hecatonOSGiInstance);
        return hecatonOSGiInstance;
    }

    private void deregisterInstance(HecatonOSGiInstance hecatonOSGiInstance) {
        instanceMap.remove(hecatonOSGiInstance.getName());
        ServiceRegistration serviceRegistration =
                instanceServiceRegistrationMap.remove(hecatonOSGiInstance);
        if (serviceRegistration != null) {
            ownerBundleContext.ungetService(serviceRegistration.getReference());
            serviceRegistration.unregister();
        }
    }

    private void shutdownDefaultHecatonInstanceIfActive() {
        if (hecatonInstance != null) {
            shutdownHecatonInstanceInternalSafely(hecatonInstance);
            hecatonInstance = null;
        }
    }

    private void shutdownAllInternal() {
        for (HecatonOSGiInstance instance : instanceMap.values()) {
            shutdownHecatonInstanceInternalSafely(instance);
        }
        // Default instance may not be registered due to set `HECATON_OSGI_REGISTER_DISABLED` flag,
        // So be sure that it is shutdown.
        shutdownDefaultHecatonInstanceIfActive();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Bundle getOwnerBundle() {
        return ownerBundle;
    }

    @Override
    public boolean isActive() {
        return ownerBundle.getState() == Bundle.ACTIVE;
    }

    @Override
    public void activate() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            if (ownerBundle.getState() == Bundle.STARTING) {
                try {
                    if (hecatonInstance != null) {
                        LOGGER.warning("Default Hecaton instance should be null while activating service!");
                        shutdownDefaultHecatonInstanceIfActive();
                    }
                    if (Boolean.getBoolean(HECATON_OSGI_START)) {
                        hecatonInstance =
                                new HecatonOSGiInstanceImpl(createHecatonInstance(null), this);
                        LOGGER.info("Default Hecaton instance has been created");
                    }
                    if (hecatonInstance != null && !Boolean.getBoolean(HECATON_OSGI_REGISTER_DISABLED)) {
                        registerInstance(hecatonInstance);
                        LOGGER.info("Default Hecaton instance has been registered as OSGI service");
                    }
                    serviceRegistration =
                            ownerBundleContext.registerService(HecatonOSGiService.class.getName(), this, null);
                    LOGGER.info(this + " has been registered as OSGI service and activated now");
                } catch (Throwable t) {
                    // If somehow default instance is activated, revert and deactivate it.
                    shutdownDefaultHecatonInstanceIfActive();
                    ExceptionUtil.rethrow(t);
                }
            }
        }
    }

    @Override
    public void deactivate() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            if (ownerBundle.getState() == Bundle.STOPPING) {
                try {
                    shutdownAllInternal();
                    try {
                        ownerBundleContext.ungetService(serviceRegistration.getReference());
                        serviceRegistration.unregister();
                    } catch (Throwable t) {
                        LOGGER.finest("Error occurred while deregistering " + this, t);
                    }
                    LOGGER.info(this + " has been deregistered as OSGI service and deactivated");
                } finally {
                    serviceRegistration = null;
                }
            }
        }
    }

    @Override
    public HecatonOSGiInstance getDefaultHecatonInstance() {
        checkActive();

        // No need to synchronization since this is not a mutating operation on instances.
        // If at this time service is deactivated, this method just returns terminated instance.

        return hecatonInstance;
    }

    @Override
    public HecatonOSGiInstance newHecatonInstance(Config config) {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            return registerInstance(createHecatonInstance(config));
        }
    }

    @Override
    public HecatonOSGiInstance newHecatonInstance() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            return registerInstance(createHecatonInstance(null));
        }
    }

    @Override
    public HecatonOSGiInstance getHecatonInstanceByName(String instanceName) {
        checkActive();

        // No need to synchronization since this is not a mutating operation on instances.
        // If at this time service is deactivated, this method just returns terminated instance.

        return instanceMap.get(instanceName);
    }

    @Override
    public Set<HecatonOSGiInstance> getAllHecatonInstances() {
        checkActive();

        // No need to synchronization since this is not a mutating operation on instances.
        // If at this time service is deactivated, this method just returns terminated instances.

        return new HashSet<>(instanceMap.values());
    }

    @Override
    public void shutdownHecatonInstance(HecatonOSGiInstance instance) {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            shutdownHecatonInstanceInternal(instance);
        }
    }

    private void shutdownHecatonInstanceInternal(HecatonOSGiInstance instance) {
        try {
            deregisterInstance(instance);
        } catch (Throwable t) {
            LOGGER.finest("Error occurred while deregistering " + instance, t);
        }
        instance.shutdown();
    }

    private void shutdownHecatonInstanceInternalSafely(HecatonOSGiInstance instance) {
        try {
            shutdownHecatonInstanceInternal(instance);
        } catch (Throwable t) {
            LOGGER.finest("Error occurred while shutting down " + instance, t);
        }
    }

    @Override
    public void shutdownAll() {
        // No need to complex lock-free approaches since this is not called frequently. Simple is good.
        synchronized (serviceMutex) {
            checkActive();

            shutdownAllInternal();
        }
    }

    @Override
    public String toString() {
        return "HecatonOSGiServiceImpl{"
                    + "ownerBundle=" + ownerBundle
                    + ", hecatonInstance=" + hecatonInstance
                    + ", active=" + isActive()
                    + ", id=" + id
                + '}';
    }

}
