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

package uk.org.hecaton.map.impl.wan;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.MutationObserver;
import uk.org.hecaton.wan.impl.merkletree.MerkleTree;

import javax.annotation.Nonnull;

/**
 * Keeps the Merkle tree of a partition of a map up to date with its entries (F13: consistency
 * check and delta WAN sync). The tree hashes the serialized keys and values, so clusters holding
 * the same entries have the same trees. Called on the partition thread, like the record store.
 */
public class MerkleTreeMutationObserver implements MutationObserver<Record> {

    private final MerkleTree tree;
    private final SerializationService serializationService;

    public MerkleTreeMutationObserver(MerkleTree tree, SerializationService serializationService) {
        this.tree = tree;
        this.serializationService = serializationService;
    }

    @Override
    public void onPutRecord(@Nonnull Data key, Record record, Object oldValue, boolean backup) {
        // a new record: an old value it replaces (e.g. an expired one) was removed from the tree already
        tree.updateAdd(key, data(record.getValue()));
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, @Nonnull Record record, boolean populateIndex) {
        tree.updateAdd(key, data(record.getValue()));
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull Record record, Object oldValue, Object newValue,
                               boolean backup) {
        if (oldValue == null) {
            tree.updateAdd(key, data(newValue));
        } else {
            tree.updateReplace(key, data(oldValue), data(newValue));
        }
    }

    @Override
    public void onRemoveRecord(@Nonnull Data key, Record record, boolean backup) {
        if (record != null) {
            tree.updateRemove(key, data(record.getValue()));
        }
    }

    @Override
    public void onEvictRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        tree.updateRemove(key, data(record.getValue()));
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        tree.updateAdd(key, data(record.getValue()));
    }

    @Override
    public void onReset() {
        tree.clear();
    }

    @Override
    public void onClear() {
        tree.clear();
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        tree.clear();
    }

    public MerkleTree getTree() {
        return tree;
    }

    private Data data(Object value) {
        return serializationService.toData(value);
    }
}
