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

package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.wan.impl.WanDataSerializerHook;
import uk.org.hecaton.wan.impl.merkletree.MerkleTreeUtil;
import uk.org.hecaton.wan.impl.merkletree.MerkleTreeView;

import java.io.IOException;

/**
 * Returns the leaf hashes of the Merkle tree of a map in a partition, or {@code null} if the map
 * keeps no Merkle tree (WAN consistency check and delta sync).
 */
public class MerkleTreeLeavesOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    private String mapName;
    private int[] leaves;

    public MerkleTreeLeavesOperation() {
    }

    MerkleTreeLeavesOperation(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() {
        MapService mapService = getNodeEngine().getService(MapService.SERVICE_NAME);
        RecordStore<?> recordStore = mapService.getMapServiceContext().getExistingRecordStore(getPartitionId(), mapName);
        MerkleTreeView tree = recordStore == null ? null : recordStore.getMerkleTree();
        if (tree != null) {
            leaves = leaves(tree);
            return;
        }
        MapConfig mapConfig = getNodeEngine().getConfig().findMapConfig(mapName);
        if (Boolean.TRUE.equals(mapConfig.getMerkleTreeConfig().getEnabled())) {
            // no entries yet: an empty tree
            leaves = new int[MerkleTreeUtil.getNodesOnLevel(mapConfig.getMerkleTreeConfig().getDepth() - 1)];
        }
    }

    static int[] leaves(MerkleTreeView tree) {
        int leafLevel = tree.depth() - 1;
        int count = MerkleTreeUtil.getNodesOnLevel(leafLevel);
        int first = MerkleTreeUtil.getLeftMostNodeOrderOnLevel(leafLevel);
        int[] leaves = new int[count];
        for (int i = 0; i < count; i++) {
            leaves[i] = tree.getNodeHash(first + i);
        }
        return leaves;
    }

    @Override
    public Object getResponse() {
        return leaves;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.MERKLE_TREE_LEAVES_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        mapName = in.readString();
    }
}
