/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.vector.module;

import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.vector.impl.VectorCollectionServiceUtil;

import java.io.IOException;

/** Rebuilds a member's indexes of a collection without their removed vectors. */
public final class VectorOptimizeOperation extends Operation {

    private String name;
    private String indexName;

    public VectorOptimizeOperation() {
    }

    VectorOptimizeOperation(String name, String indexName) {
        this.name = name;
        this.indexName = indexName;
    }

    @Override
    public String getServiceName() {
        return VectorCollectionServiceUtil.SERVICE_NAME;
    }

    @Override
    public void run() {
        VectorCollectionService service = getService();
        service.optimizeLocal(name, indexName);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(indexName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        indexName = in.readString();
    }
}
