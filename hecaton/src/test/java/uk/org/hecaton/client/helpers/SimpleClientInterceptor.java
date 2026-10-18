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

package uk.org.hecaton.client.helpers;

import uk.org.hecaton.map.MapInterceptorAdaptor;
import uk.org.hecaton.nio.serialization.Portable;
import uk.org.hecaton.nio.serialization.PortableReader;
import uk.org.hecaton.nio.serialization.PortableWriter;
import uk.org.hecaton.internal.util.StringUtil;

import java.io.IOException;
import java.io.Serial;


/**
 * User: danny Date: 11/26/13
 */

public class SimpleClientInterceptor extends MapInterceptorAdaptor implements Portable {
    @Serial
    private static final long serialVersionUID = 1L;

    public static final int ID = 345;

    @Override
    public Object interceptGet(Object value) {
        if (value == null) {
            return null;
        }
        return value + ":";
    }

    @Override
    public Object interceptPut(Object oldValue, Object newValue) {
        return StringUtil.upperCaseInternal(newValue.toString());
    }

    @Override
    public Object interceptRemove(Object removedValue) {
        if (removedValue.equals("ISTANBUL")) {
            throw new RuntimeException("you can not remove this");
        }
        return super.interceptRemove(removedValue);
    }

    @Override
    public int getFactoryId() {
        return PortableHelpersFactory.ID;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getClassId() {
        return ID;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
