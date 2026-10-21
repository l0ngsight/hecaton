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

import uk.org.hecaton.internal.cluster.impl.operations.WanOperation;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.wan.impl.WanDataSerializerHook;
import uk.org.hecaton.wan.impl.WanReplicationService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Sent by a publisher to a member of the target cluster: returns, for the given partitions, the leaf
 * hashes of the Merkle trees of a map in the target cluster (a {@code Map<Integer, int[]>}; a
 * partition whose map keeps no Merkle tree is missing).
 */
public class WanMerkleTreeQueryOperation extends Operation implements WanOperation, IdentifiedDataSerializable {

    private String targetClusterName;
    private String mapName;
    private int[] partitionIds;

    public WanMerkleTreeQueryOperation() {
    }

    WanMerkleTreeQueryOperation(String targetClusterName, String mapName, int[] partitionIds) {
        this.targetClusterName = targetClusterName;
        this.mapName = mapName;
        this.partitionIds = partitionIds;
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public void run() {
        String clusterName = getNodeEngine().getConfig().getClusterName();
        if (!clusterName.equals(targetClusterName)) {
            throw new IllegalStateException("A WAN publisher queried cluster '" + targetClusterName
                    + "' on a member of cluster '" + clusterName + "'");
        }
        OperationService operationService = getNodeEngine().getOperationService();
        Map<Integer, InternalCompletableFuture<int[]>> futures = new HashMap<>();
        for (int partitionId : partitionIds) {
            futures.put(partitionId, operationService.invokeOnPartition(
                    new MerkleTreeLeavesOperation(mapName).setPartitionId(partitionId)));
        }
        CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).whenComplete((ignored, failure) -> {
            if (failure != null) {
                sendResponse(failure);
                return;
            }
            HashMap<Integer, int[]> leaves = new HashMap<>();
            futures.forEach((partitionId, future) -> {
                int[] partitionLeaves = future.join();
                if (partitionLeaves != null) {
                    leaves.put(partitionId, partitionLeaves);
                }
            });
            sendResponse(leaves);
        });
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_MERKLE_TREE_QUERY_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(targetClusterName);
        out.writeString(mapName);
        out.writeIntArray(partitionIds);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        targetClusterName = in.readString();
        mapName = in.readString();
        partitionIds = in.readIntArray();
    }
}
