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

package uk.org.hecaton.internal.serialization.impl;

import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.nio.serialization.impl.Versioned;
import uk.org.hecaton.version.Version;

import java.io.IOException;

/**
 * Sample object to test versioned data input/output
 */
public class VersionedDataSerializable implements DataSerializable, Versioned {

    private Version version;


    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        this.version = out.getVersion();
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        this.version = in.getVersion();
    }

    public Version getVersion() {
        return version;
    }
}
