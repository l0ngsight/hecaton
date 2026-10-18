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
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.osgi.impl.HecatonInternalOSGiService;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.osgi.framework.BundleException;
import org.osgi.framework.ServiceReference;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class HecatonOSGiServiceTest extends HecatonTestSupport {

    private TestBundle bundle;
    private TestBundleContext bundleContext;
    private TestBundleRegisterDeregisterListener registerDeregisterListener;

    private class TestBundleRegisterDeregisterListener implements TestBundle.RegisterDeregisterListener {

        final String className;

        boolean throwExceptionOnRegister;
        boolean throwExceptionOnDeregister;

        private TestBundleRegisterDeregisterListener() {
            this.className = null;
        }

        private TestBundleRegisterDeregisterListener(String className) {
            this.className = className;
        }

        @Override
        public void onRegister(String clazz, TestServiceReference serviceReference) {
            if (throwExceptionOnRegister) {
                if (className != null) {
                    if (clazz.equals(className)) {
                        throw new IllegalStateException("You cannot register!");
                    }
                } else {
                    throw new IllegalStateException("You cannot register!");
                }
            }
        }

        @Override
        public void onDeregister(String clazz, TestServiceReference serviceReference) {
            if (throwExceptionOnDeregister) {
                if (className != null) {
                    if (clazz.equals(className)) {
                        throw new IllegalStateException("You cannot deregister!");
                    }
                } else {
                    throw new IllegalStateException("You cannot deregister!");
                }
            }
        }
    }

    @Before
    public void setup() throws BundleException {
        registerDeregisterListener = new TestBundleRegisterDeregisterListener();
        bundle = new TestBundle(registerDeregisterListener);
        bundleContext = bundle.getBundleContext();
        bundle.start();
    }

    @After
    public void tearDown() throws BundleException {
        try {
            bundle.stop();
            bundle = null;
            registerDeregisterListener = null;
            bundleContext = null;
        } finally {
            Hecaton.shutdownAll();
        }
    }

    private HecatonInternalOSGiService getService(TestBundleContext bundleContext) {
        ServiceReference serviceRef = bundleContext.getServiceReference(HecatonOSGiService.class.getName());
        if (serviceRef == null) {
            return null;
        }
        return (HecatonInternalOSGiService) bundleContext.getService(serviceRef);
    }

    private HecatonInternalOSGiService getService() {
        return getService(bundleContext);
    }

    @Test
    public void serviceRetrievedSuccessfully() {
        HecatonInternalOSGiService service = getService();

        assertNotNull(service);
    }

    @Test
    public void bundleOfServiceRetrievedSuccessfully() {
        HecatonInternalOSGiService service = getService();

        assertEquals(bundle, service.getOwnerBundle());
    }

    @Test
    public void idOfServiceRetrievedSuccessfully() {
        HecatonInternalOSGiService service = getService();

        assertEquals(HecatonInternalOSGiService.DEFAULT_ID, service.getId());
    }

    @Test
    public void serviceDeactivatedAndThenActivatedSuccessfully() throws BundleException {
        HecatonInternalOSGiService service = getService();

        assertTrue(service.isActive());

        bundle.stop();

        assertFalse(service.isActive());
        assertNull(getService());

        bundle.start();

        assertTrue(service.isActive());
        assertNotNull(getService());
    }

    @Test
    public void defaultInstanceNotExistWhenItIsNotSpecified() {
        HecatonInternalOSGiService service = getService();

        assertNull(service.getDefaultHecatonInstance());
    }

    @Test
    public void defaultInstanceExistWhenItIsSpecified() throws BundleException {
        String propValue = System.getProperty(HecatonOSGiService.HECATON_OSGI_START);
        TestBundle testBundle = null;
        try {
            System.setProperty(HecatonOSGiService.HECATON_OSGI_START, "true");

            testBundle = new TestBundle();
            testBundle.start();

            HecatonInternalOSGiService service = getService(testBundle.getBundleContext());
            assertNotNull(service);

            assertNotNull(service.getDefaultHecatonInstance());
        } finally {
            if (propValue != null) {
                System.setProperty(HecatonOSGiService.HECATON_OSGI_START, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void serviceCouldNotBeActivatedWhenThereIsExceptionWhileRegisteringDefaultInstance() throws BundleException {
        String propValue = System.getProperty(HecatonOSGiService.HECATON_OSGI_START);
        TestBundle testBundle = null;
        try {
            System.setProperty(HecatonOSGiService.HECATON_OSGI_START, "true");

            TestBundleRegisterDeregisterListener registerDeregisterListener =
                    new TestBundleRegisterDeregisterListener(HecatonInstance.class.getName());
            registerDeregisterListener.throwExceptionOnRegister = true;

            testBundle = new TestBundle(registerDeregisterListener);
            try {
                testBundle.start();
                fail("OSGI service could not be activated because of exception while registering default instance."
                        + " It is expected to get `IllegalStateException` here!");
            } catch (IllegalStateException e) {
                // Since bundle is not active, it is expected to get `IllegalStateException`
            }
        } finally {
            if (propValue != null) {
                System.setProperty(HecatonOSGiService.HECATON_OSGI_START, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void serviceCouldNotBeActivatedWhenThereIsExceptionWhileRegisteringService() throws BundleException {
        String propValue = System.getProperty(HecatonOSGiService.HECATON_OSGI_START);
        TestBundle testBundle = null;
        try {
            System.setProperty(HecatonOSGiService.HECATON_OSGI_START, "true");

            TestBundleRegisterDeregisterListener registerDeregisterListener =
                    new TestBundleRegisterDeregisterListener(HecatonOSGiService.class.getName());
            registerDeregisterListener.throwExceptionOnRegister = true;

            testBundle = new TestBundle(registerDeregisterListener);
            try {
                testBundle.start();
                fail("OSGI service could not be activated because of exception while registering default instance."
                        + "It is expected to get `IllegalStateException` here!");
            } catch (IllegalStateException e) {
                // Since bundle is not active, it is expected to get `IllegalStateException`
            }
        } finally {
            if (propValue != null) {
                System.setProperty(HecatonOSGiService.HECATON_OSGI_START, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void newInstanceRetrievedSuccessfullyWithoutConfiguration() {
        HecatonInternalOSGiService service = getService();

        HecatonOSGiInstance osgiInstance = service.newHecatonInstance();
        assertNotNull(osgiInstance);

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
    }

    @Test
    public void newInstanceRetrievedSuccessfullyWithConfiguration() {
        final String INSTANCE_NAME = "test-osgi-instance";

        HecatonInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        HecatonOSGiInstance osgiInstance = service.newHecatonInstance(config);
        assertNotNull(osgiInstance);
        assertEquals(config.getInstanceName(), osgiInstance.getConfig().getInstanceName());

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertEquals(config.getInstanceName(), instance.getConfig().getInstanceName());
    }

    @Test
    public void newInstanceRegisteredAsServiceWhenRegistrationIsNotDisabled() throws BundleException {
        HecatonInternalOSGiService service = getService();

        service.newHecatonInstance();

        assertNull(bundleContext.getServiceReference(HecatonOSGiInstance.class.getName()));
    }

    @Test
    public void newInstanceNotRegisteredAsServiceWhenRegistrationIsDisabled() throws BundleException {
        String propValue = System.getProperty(HecatonOSGiService.HECATON_OSGI_REGISTER_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HecatonOSGiService.HECATON_OSGI_REGISTER_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HecatonInternalOSGiService service = getService(testBundleContext);
            assertNotNull(service);

            service.newHecatonInstance();

            assertNull(testBundleContext.getServiceReference(HecatonOSGiInstance.class.getName()));
        } finally {
            if (propValue != null) {
                System.setProperty(HecatonOSGiService.HECATON_OSGI_REGISTER_DISABLED, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void clusterNameIsSetToDefaultClusterNameOfBundleWhenGroupingIsNotDisabled() {
        HecatonInternalOSGiService service = getService();

        HecatonOSGiInstance osgiInstance = service.newHecatonInstance();
        assertEquals(HecatonInternalOSGiService.DEFAULT_CLUSTER_NAME,
                osgiInstance.getConfig().getClusterName());

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertEquals(HecatonInternalOSGiService.DEFAULT_CLUSTER_NAME,
                instance.getConfig().getClusterName());
    }

    @Test
    public void clusterNameIsSetToDefaultClusterNameOfBundleWhenConfigIsGivenWithoutSpecifiedClusterConfigAndGroupingIsNotDisabled() {
        Config config = new Config();

        HecatonInternalOSGiService service = getService();

        HecatonOSGiInstance osgiInstance = service.newHecatonInstance(config);
        assertEquals(HecatonInternalOSGiService.DEFAULT_CLUSTER_NAME,
                osgiInstance.getConfig().getClusterName());

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertEquals(HecatonInternalOSGiService.DEFAULT_CLUSTER_NAME,
                instance.getConfig().getClusterName());
    }

    @Test
    public void clusterNameIsSetToDefaultClusterNameOfBundleWhenConfigIsGivenWithNullGroupConfigAndGroupingIsNotDisabled() {
        Config config = new Config();

        HecatonInternalOSGiService service = getService();

        HecatonOSGiInstance osgiInstance = service.newHecatonInstance(config);
        assertEquals(HecatonInternalOSGiService.DEFAULT_CLUSTER_NAME,
                osgiInstance.getConfig().getClusterName());

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertEquals(HecatonInternalOSGiService.DEFAULT_CLUSTER_NAME,
                instance.getConfig().getClusterName());
    }

    @Test
    public void clusterNameIsSetToSpecifiedClusterNameWhenGroupingIsNotDisabled() {
        final String GROUP_NAME = "my-osgi-group";

        HecatonInternalOSGiService service = getService();

        Config config = new Config();
        config.setClusterName(GROUP_NAME);

        HecatonOSGiInstance osgiInstance = service.newHecatonInstance(config);
        assertEquals(GROUP_NAME,
                osgiInstance.getConfig().getClusterName());

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertEquals(GROUP_NAME,
                instance.getConfig().getClusterName());
    }

    @Test
    public void clusterNameIsSetToDefaultClusterNameWhenGroupingIsDisabled() throws BundleException {
        String propValue = System.getProperty(HecatonOSGiService.HECATON_OSGI_GROUPING_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HecatonOSGiService.HECATON_OSGI_GROUPING_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HecatonInternalOSGiService service = getService(testBundleContext);
            assertNotNull(service);

            HecatonOSGiInstance osgiInstance = service.newHecatonInstance();
            assertEquals(Config.DEFAULT_CLUSTER_NAME, osgiInstance.getConfig().getClusterName());

            HecatonInstance instance = osgiInstance.getDelegatedInstance();
            assertEquals(Config.DEFAULT_CLUSTER_NAME, instance.getConfig().getClusterName());
        } finally {
            if (propValue != null) {
                System.setProperty(HecatonOSGiService.HECATON_OSGI_GROUPING_DISABLED, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void clusterNameIsSetToSpecifiedClusterNameWhenGroupingIsDisabled() throws BundleException {
        final String CLUSTER_NAME = "my-osgi-cluster";

        String propValue = System.getProperty(HecatonOSGiService.HECATON_OSGI_GROUPING_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HecatonOSGiService.HECATON_OSGI_GROUPING_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HecatonInternalOSGiService service = getService(testBundleContext);
            assertNotNull(service);

            Config config = new Config();
            config.setClusterName(CLUSTER_NAME);

            HecatonOSGiInstance osgiInstance = service.newHecatonInstance(config);
            assertEquals(CLUSTER_NAME, osgiInstance.getConfig().getClusterName());

            HecatonInstance instance = osgiInstance.getDelegatedInstance();
            assertEquals(CLUSTER_NAME, instance.getConfig().getClusterName());
        } finally {
            if (propValue != null) {
                System.setProperty(HecatonOSGiService.HECATON_OSGI_GROUPING_DISABLED, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void instanceRetrievedSuccessfullyWithItsName() {
        final String INSTANCE_NAME = "test-osgi-instance";

        HecatonInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        service.newHecatonInstance(config);

        HecatonOSGiInstance osgiInstance = service.getHecatonInstanceByName(INSTANCE_NAME);
        assertNotNull(osgiInstance);
        assertEquals(config.getInstanceName(), osgiInstance.getConfig().getInstanceName());

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertEquals(config.getInstanceName(), instance.getConfig().getInstanceName());
    }

    @Test
    public void allInstancesRetrievedSuccessfully() {
        Set<HecatonOSGiInstance> osgiInstances = new HashSet<>();

        HecatonInternalOSGiService service = getService();

        osgiInstances.add(service.newHecatonInstance());
        osgiInstances.add(service.newHecatonInstance(new Config("test-osgi-instance")));

        Set<HecatonOSGiInstance> allOSGiInstances = service.getAllHecatonInstances();
        assertEquals(osgiInstances.size(), allOSGiInstances.size());
        assertContainsAll(allOSGiInstances, osgiInstances);
    }

    @Test
    public void instanceShutdownSuccessfully() {
        final String INSTANCE_NAME = "test-osgi-instance";

        HecatonInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        HecatonOSGiInstance osgiInstance = service.newHecatonInstance(config);
        assertTrue(osgiInstance.getLifecycleService().isRunning());

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertTrue(instance.getLifecycleService().isRunning());

        service.shutdownHecatonInstance(osgiInstance);

        assertFalse(osgiInstance.getLifecycleService().isRunning());
        assertFalse(instance.getLifecycleService().isRunning());
        assertNull(service.getHecatonInstanceByName(INSTANCE_NAME));
        assertFalse(service.getAllHecatonInstances().contains(osgiInstance));
    }

    @Test
    public void instanceShutdownSuccessfullyAlthoughThereIsExceptionWhileDeregister() {
        final String INSTANCE_NAME = "test-osgi-instance";

        registerDeregisterListener.throwExceptionOnDeregister = true;

        HecatonInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        HecatonOSGiInstance osgiInstance = service.newHecatonInstance(config);
        assertTrue(osgiInstance.getLifecycleService().isRunning());

        HecatonInstance instance = osgiInstance.getDelegatedInstance();
        assertTrue(instance.getLifecycleService().isRunning());

        service.shutdownHecatonInstance(osgiInstance);

        assertFalse(osgiInstance.getLifecycleService().isRunning());
        assertFalse(instance.getLifecycleService().isRunning());
        assertNull(service.getHecatonInstanceByName(INSTANCE_NAME));
        assertFalse(service.getAllHecatonInstances().contains(osgiInstance));
    }

    @Test
    public void allInstancesShutdownSuccessfully() {
        Set<HecatonOSGiInstance> osgiInstances = new HashSet<>();

        HecatonInternalOSGiService service = getService();

        osgiInstances.add(service.newHecatonInstance());
        osgiInstances.add(service.newHecatonInstance(new Config("test-osgi-instance")));

        Set<HecatonOSGiInstance> allOSGiInstances = service.getAllHecatonInstances();
        for (HecatonOSGiInstance osgiInstance : allOSGiInstances) {
            assertTrue(osgiInstance.getLifecycleService().isRunning());
            HecatonInstance instance = osgiInstance.getDelegatedInstance();
            assertTrue(instance.getLifecycleService().isRunning());
        }

        service.shutdownAll();

        for (HecatonOSGiInstance osgiInstance : osgiInstances) {
            assertFalse(osgiInstance.getLifecycleService().isRunning());
            HecatonInstance instance = osgiInstance.getDelegatedInstance();
            assertFalse(instance.getLifecycleService().isRunning());
        }

        allOSGiInstances = service.getAllHecatonInstances();
        assertEquals(0, allOSGiInstances.size());
    }

    @Test
    public void allInstancesShutdownSuccessfullyAlthoughThereIsExceptionWhileDeregister() {
        Set<HecatonOSGiInstance> osgiInstances = new HashSet<>();

        registerDeregisterListener.throwExceptionOnDeregister = true;

        HecatonInternalOSGiService service = getService();

        osgiInstances.add(service.newHecatonInstance());
        osgiInstances.add(service.newHecatonInstance(new Config("test-osgi-instance")));

        Set<HecatonOSGiInstance> allOSGiInstances = service.getAllHecatonInstances();
        for (HecatonOSGiInstance osgiInstance : allOSGiInstances) {
            assertTrue(osgiInstance.getLifecycleService().isRunning());
            HecatonInstance instance = osgiInstance.getDelegatedInstance();
            assertTrue(instance.getLifecycleService().isRunning());
        }

        service.shutdownAll();

        for (HecatonOSGiInstance osgiInstance : osgiInstances) {
            assertFalse(osgiInstance.getLifecycleService().isRunning());
            HecatonInstance instance = osgiInstance.getDelegatedInstance();
            assertFalse(instance.getLifecycleService().isRunning());
        }

        allOSGiInstances = service.getAllHecatonInstances();
        assertEquals(0, allOSGiInstances.size());
    }

    @Test
    public void serviceIsNotOperationalWhenItIsNotActive() throws BundleException {
        TestBundle testBundle = null;
        try {
            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HecatonInternalOSGiService service = getService(testBundleContext);
            assertNotNull(service);

            testBundle.stop();
            testBundle = null;

            try {
                service.newHecatonInstance();
                fail("OSGI service is not active so it is not in operation mode."
                        + " It is expected to get `IllegalStateException` here!");
            } catch (IllegalStateException e) {
                // since the bundle is not active, it is expected to get `IllegalStateException`
            }
        } finally {
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }
}
