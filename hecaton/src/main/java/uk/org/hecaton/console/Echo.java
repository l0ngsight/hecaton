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

package uk.org.hecaton.console;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.BinaryInterface;
import uk.org.hecaton.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Echoes to screen.
 */
@BinaryInterface
public class Echo implements Callable<String>, DataSerializable, HecatonInstanceAware {

    String input;

    private transient HecatonInstance hz;

    public Echo() {
        // no-arg constructor needed for DataSerializable classes
    }

    public Echo(String input) {
        this.input = input;
    }

    @Override
    public String call() {
        return hz.getCluster().getLocalMember() + ":" + input;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(input);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        input = in.readString();
    }

    @Override
    public void setHecatonInstance(HecatonInstance hecatonInstance) {
        this.hz = hecatonInstance;
    }
}
