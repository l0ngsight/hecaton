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

package uk.org.hecaton.multimap.impl.operations;

import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.multimap.impl.MultiMapMergeContainer;
import uk.org.hecaton.multimap.impl.MultiMapRecord;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.multimap.impl.MultiMapValue;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.MultiMapMergeTypes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.internal.util.MapUtil.createHashMap;

/**
 * Merges multiple {@link MultiMapMergeContainer} for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends AbstractMultiMapOperation implements BackupAwareOperation {

    private List<MultiMapMergeContainer> mergeContainers;
    private SplitBrainMergePolicy<Collection<Object>, MultiMapMergeTypes<Object, Object>, Collection<Object>> mergePolicy;

    private transient Map<Data, Collection<MultiMapRecord>> resultMap;

    public MergeOperation() {
    }

    public MergeOperation(String name, List<MultiMapMergeContainer> mergeContainers,
                          SplitBrainMergePolicy<Collection<Object>, MultiMapMergeTypes<Object, Object>,
                                  Collection<Object>> mergePolicy) {
        super(name);
        this.mergeContainers = mergeContainers;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        resultMap = createHashMap(mergeContainers.size());
        for (MultiMapMergeContainer mergeContainer : mergeContainers) {
            Data key = mergeContainer.getKey();
            if (!container.canAcquireLock(key, getCallerUuid(), -1)) {
                Object valueKey = getNodeEngine().getSerializationService().toObject(key);
                getLogger().info("Skipped merging of locked key '" + valueKey + "' on MultiMap '" + name + "'");
                continue;
            }

            MultiMapValue result = container.merge(mergeContainer, mergePolicy);
            if (result != null) {
                resultMap.put(key, result.getCollection(false));
            }
        }
        response = !resultMap.isEmpty();
    }

    @Override
    public boolean shouldBackup() {
        return !resultMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new MergeBackupOperation(name, resultMap);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        SerializationUtil.writeList(mergeContainers, out);
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergeContainers = SerializationUtil.readList(in);
        mergePolicy = NamespaceUtil.callWithNamespace(in::readObject, name, MultiMapService::lookupNamespace);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.MERGE_OPERATION;
    }
}
