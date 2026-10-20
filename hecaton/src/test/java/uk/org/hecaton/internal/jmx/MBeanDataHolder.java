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

package uk.org.hecaton.internal.jmx;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.TestHecatonInstanceFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.internal.jmx.ManagementService.quote;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Holds the Hecaton instance and MBean server and provides some utility functions for accessing the MBeans.
 */
public final class MBeanDataHolder {

    private static final AtomicInteger ID_GEN = new AtomicInteger();

    private HecatonInstance hz;
    private MBeanServer mbs;

    /**
     * Initialize with new hecaton instance and MBean server
     */
    public MBeanDataHolder(TestHecatonInstanceFactory factory) {
       this(factory, new Config());
    }

    public MBeanDataHolder(TestHecatonInstanceFactory factory, Config config) {
        config.setInstanceName("hz:\",=*?" + ID_GEN.getAndIncrement());
        config.setProperty(ClusterProperty.ENABLE_JMX.getName(), "true");
        hz = factory.newHecatonInstance(config);
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public MBeanDataHolder(HecatonInstance instance) {
        hz = instance;
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public void assertMBeanExistEventually(final String type, final String name) {
        assertTrueEventually(() -> {
            ObjectName object = getObjectName(type, name);
            try {
                mbs.getObjectInstance(object);
            } catch (InstanceNotFoundException e) {
                fail(e.getMessage());
            }
        });
    }

    public void assertMBeanNotExistEventually(final String type, final String name) {
        assertTrueEventually(() -> {
            ObjectName object = getObjectName(type, name);
            assertFalse(mbs.isRegistered(object));
        });
    }

    /**
     * @return Hecaton instance managed by this holder
     */
    public HecatonInstance getHz() {
        return hz;
    }

    /**
     * @return MBean server managed by this holder
     */
    public MBeanServer getMbs() {
        return mbs;
    }

    /**
     * Get the value of the JMX attribute with the specified name
     *
     * @param type          Type of the Hecaton object (first level of hierarchy), e.g. "IMap"
     * @param objectName    Name of the Hecaton object (second level of hierarchy), e.g. "myMap"
     * @param attributeName Name of attribute to query, e.g. "size"
     * @return Value to get
     */
    public Object getMBeanAttribute(final String type, final String objectName, String attributeName) throws Exception {
        return mbs.getAttribute(getObjectName(type, objectName), attributeName);
    }

    /**
     * Calls the MBean operation with given attributes
     *
     * @param type          Type of the Hecaton object (first level of hierarchy), e.g. "IMap"
     * @param objectName    Name of the Hecaton object (second level of hierarchy), e.g. "myMap"
     * @param operationName Name of attribute to query, e.g. "size"
     * @param params        Parameters for call. May be null for methods without parameters.
     * @param signature     Class names of parameters, see {@link javax.management.MBeanServerConnection#invoke(javax.management.ObjectName, String, Object[], String[])}
     *                      May be null for methods without parameters.
     * @return Value to get
     */
    public Object invokeMBeanOperation(final String type, final String objectName, String operationName, Object[] params,
                                       String[] signature) throws Exception {
        return mbs.invoke(getObjectName(type, objectName), operationName, params, signature);
    }

    /**
     * JMX object name
     *
     * @param type       Type of the Hecaton object (first level of hierarchy), e.g. "IMap"
     * @param objectName Name of the Hecaton object (second level of hierarchy), e.g. "myMap"
     * @return JMX object name
     */
    private ObjectName getObjectName(String type, String objectName) throws MalformedObjectNameException {
        Hashtable<String, String> table = new Hashtable<>();
        table.put("type", quote(type));
        table.put("name", quote(objectName));
        table.put("instance", quote(hz.getName()));
        return new ObjectName("uk.org.hecaton", table);
    }
}
