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

package uk.org.hecaton.query;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.instance.impl.HecatonInstanceProxy;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.services.NodeAware;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class ReflectiveAttributeTestObject implements Serializable, HecatonInstanceAware, NodeAware {
    public static Integer staticInt = 456;

    private transient Node node;
    private transient HecatonInstance hecatonInstance;
    private transient HecatonInstanceProxy hecatonInstanceProxy;

    private final String name;
    private final boolean allowTransientFieldSetting;
    private final ReflectiveAttributeTestObject childNode;

    private final Collection<ReflectiveAttributeTestObject> objectAsList;
    private final ReflectiveAttributeTestObject[] objectAsArray;

    private final HecatonInstance[] hecatonInstanceArray = new HecatonInstance[1];
    private final HecatonInstance[][][] hecatonInstanceMultiDimArray = new HecatonInstance[1][][];
    private final List<HecatonInstance> hecatonInstanceList = new ArrayList<>(1);

    public ReflectiveAttributeTestObject(String name) {
        this(name, null, null, null, true);
    }

    public ReflectiveAttributeTestObject(String name, boolean allowTransientFieldSetting) {
        this(name, null, null, null, allowTransientFieldSetting);
    }

    public ReflectiveAttributeTestObject(String name,
                                         ReflectiveAttributeTestObject childNode,
                                         Collection<ReflectiveAttributeTestObject> pathElementsList,
                                         ReflectiveAttributeTestObject[] pathElementsArray,
                                         boolean allowTransientFieldSetting) {
        this.name = name;
        this.childNode = childNode == null ? this : childNode;
        this.objectAsList = pathElementsList;
        this.objectAsArray = pathElementsArray;
        this.allowTransientFieldSetting = allowTransientFieldSetting;
    }

    public static ReflectiveAttributeTestObject withList(String name,
                                                         ReflectiveAttributeTestObject... elements) {
        return new ReflectiveAttributeTestObject(name, null, Arrays.asList(elements), null, true);
    }

    public static ReflectiveAttributeTestObject withArray(String name,
                                                          ReflectiveAttributeTestObject... elements) {
        return new ReflectiveAttributeTestObject(name, null, null, elements, true);
    }

    public String getName() {
        return name;
    }

    public static String name() {
        return "static";
    }

    public static Integer getStaticValue() {
        return 123;
    }

    public ReflectiveAttributeTestObject child() {
        return childNode;
    }

    public static ReflectiveAttributeTestObject staticChild() {
        return new ReflectiveAttributeTestObject("staticChild");
    }

    public Collection<ReflectiveAttributeTestObject> getObjectAsList() {
        return objectAsList;
    }

    public ReflectiveAttributeTestObject[] getObjectAsArray() {
        return objectAsArray;
    }

    public void doVoid() {
    }

    public String value() {
        return name;
    }

    public HecatonInstance getHecatonInstance() {
        return hecatonInstance;
    }

    @Override
    public void setHecatonInstance(HecatonInstance hecatonInstance) {
        if (allowTransientFieldSetting) {
            this.hecatonInstance = hecatonInstance;
            this.hecatonInstanceArray[0] = hecatonInstance;
            this.hecatonInstanceList.add(hecatonInstance);
            try {
                if (hecatonInstance instanceof HecatonInstanceImpl hzImpl) {
                    Constructor<HecatonInstanceProxy> constructor = HecatonInstanceProxy.class
                            .getDeclaredConstructor(HecatonInstanceImpl.class);
                    constructor.setAccessible(true);
                    this.hecatonInstanceProxy = constructor.newInstance(hzImpl);
                }
            } catch (ReflectiveOperationException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void setNode(Node node) {
        if (allowTransientFieldSetting) {
            this.node = node;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReflectiveAttributeTestObject)) {
            return false;
        }
        ReflectiveAttributeTestObject that = (ReflectiveAttributeTestObject) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
