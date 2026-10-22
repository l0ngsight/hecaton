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

import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.module.MemberModule;
import uk.org.hecaton.instance.impl.module.VectorCapability;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.MutationObserver;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.vector.impl.VectorCollectionServiceUtil;

import javax.annotation.Nullable;

/**
 * The vector collection module ({@code hecaton-vector}): stores the documents of a vector collection
 * in an IMap ({@value VectorCapability#BACKING_MAP_PREFIX}{@code <name>}) and indexes their vectors
 * in a graph per partition and index.
 */
public class VectorModule implements MemberModule, VectorCapability {

    private volatile Node node;

    @Override
    public String name() {
        return "vector";
    }

    @Override
    public void init(Node node) {
        this.node = node;
    }

    @Nullable
    @Override
    public MutationObserver<Record> createMapObserver(RecordStore<Record> recordStore) {
        VectorCollectionService service = node.getNodeEngine().getService(VectorCollectionServiceUtil.SERVICE_NAME);
        return service.createObserver(recordStore);
    }
}
