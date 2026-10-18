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

package uk.org.hecaton.projection.impl;

import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.projection.Projection;

public final class IdentityProjection<I> implements Projection<I, I>, IdentifiedDataSerializable {

    public static final IdentityProjection<Object> INSTANCE = new IdentityProjection<>();

    private IdentityProjection() {
    }

    @Override
    public I transform(I input) {
        return input;
    }

    @Override
    public int getFactoryId() {
        return ProjectionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ProjectionDataSerializerHook.IDENTITY_PROJECTION;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }
}
