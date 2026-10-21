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

import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;
import uk.org.hecaton.vector.SearchOptions;
import uk.org.hecaton.vector.VectorValues;
import uk.org.hecaton.vector.impl.DataSearchResult;
import uk.org.hecaton.vector.impl.VectorCollectionServiceUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Searches the partitions a member owns. */
public final class VectorSearchOperation extends Operation implements ReadonlyOperation {

    private String name;
    private VectorValues query;
    private SearchOptions options;
    private transient Results results;

    public VectorSearchOperation() {
    }

    VectorSearchOperation(String name, VectorValues query, SearchOptions options) {
        this.name = name;
        this.query = query;
        this.options = options;
    }

    @Override
    public String getServiceName() {
        return VectorCollectionServiceUtil.SERVICE_NAME;
    }

    @Override
    public void run() {
        VectorCollectionService service = getService();
        results = new Results(service.searchLocal(name, query, options));
    }

    @Override
    public Object getResponse() {
        return results;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(query);
        out.writeObject(options);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        query = in.readObject();
        options = in.readObject();
    }

    /** The results of a member, with all the vectors of the documents (when included). */
    public static final class Results implements DataSerializable {

        private List<DataSearchResult> list;

        public Results() {
        }

        Results(List<DataSearchResult> list) {
            this.list = list;
        }

        List<DataSearchResult> list() {
            return list;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(list.size());
            for (DataSearchResult result : list) {
                IOUtil.writeData(out, result.getKey());
                IOUtil.writeData(out, result.getValue());
                out.writeFloat(result.getScore());
                out.writeObject(result.getVectors());
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(new DataSearchResult(IOUtil.readData(in), IOUtil.readData(in), in.readFloat(), in.readObject()));
            }
        }
    }
}
