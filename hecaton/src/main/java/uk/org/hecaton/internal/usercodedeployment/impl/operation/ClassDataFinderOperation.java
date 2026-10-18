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

package uk.org.hecaton.internal.usercodedeployment.impl.operation;

import uk.org.hecaton.internal.usercodedeployment.UserCodeDeploymentService;
import uk.org.hecaton.internal.usercodedeployment.impl.ClassData;
import uk.org.hecaton.internal.usercodedeployment.impl.UserCodeDeploymentSerializerHook;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.UrgentSystemOperation;

import java.io.IOException;

public final class ClassDataFinderOperation extends Operation implements UrgentSystemOperation, IdentifiedDataSerializable {

    private String className;
    private ClassData response;

    public ClassDataFinderOperation(String className) {
        this.className = className;
    }

    public ClassDataFinderOperation() {
    }

    @Override
    public ClassData getResponse() {
        return response;
    }

    @Override
    public void run() throws Exception {
        UserCodeDeploymentService service = getService();
        response = service.getClassDataOrNull(className);
    }

    @Override
    public String getServiceName() {
        return UserCodeDeploymentService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(className);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        className = in.readString();
    }

    @Override
    public int getFactoryId() {
        return UserCodeDeploymentSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return UserCodeDeploymentSerializerHook.CLASS_DATA_FINDER_OP;
    }
}
