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

package uk.org.hecaton.ringbuffer.impl.operations;

import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.ringbuffer.OverflowPolicy;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Notifier;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;

import static uk.org.hecaton.internal.namespace.NamespaceUtil.callWithNamespace;
import static uk.org.hecaton.ringbuffer.OverflowPolicy.FAIL;
import static uk.org.hecaton.ringbuffer.impl.RingbufferDataSerializerHook.ADD_OPERATION;

/**
 * Adds a new ring buffer item. The master node will add the item into the ring buffer, generating a new sequence ID while
 * the backup operation will put the item under the sequence ID that the master generated. This is to avoid differences
 * in ring buffer data structures.
 */
public class AddOperation extends AbstractRingBufferOperation implements Notifier, BackupAwareOperation, MutatingOperation {

    private Data item;
    private long resultSequence;
    private OverflowPolicy overflowPolicy;

    public AddOperation() {
    }

    public AddOperation(String name, Data item, OverflowPolicy overflowPolicy) {
        super(name);
        this.item = item;
        this.overflowPolicy = overflowPolicy;
    }

    @Override
    public void run() throws Exception {
        RingbufferContainer ringbuffer = getRingBufferContainer();

        //todo: move into ringbuffer.
        if (overflowPolicy == FAIL) {
            if (ringbuffer.remainingCapacity() < 1) {
                resultSequence = -1;
                return;
            }
        }

        resultSequence = callWithNamespace(ringbuffer.getUserCodeNamespace(), () -> ringbuffer.add(item));
    }

    @Override
    public void afterRun() throws Exception {
        reportReliableTopicPublish(1);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getRingbufferWaitNotifyKey();
    }

    @Override
    public boolean shouldNotify() {
        return resultSequence != -1;
    }

    @Override
    public boolean shouldBackup() {
        return resultSequence != -1;
    }

    @Override
    public int getSyncBackupCount() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        return ringbuffer.getConfig().getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        return ringbuffer.getConfig().getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new AddBackupOperation(name, resultSequence, item);
    }

    @Override
    public Long getResponse() {
        return resultSequence;
    }

    @Override
    public int getClassId() {
        return ADD_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, item);
        out.writeInt(overflowPolicy.getId());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        item = IOUtil.readData(in);
        overflowPolicy = OverflowPolicy.getById(in.readInt());
    }
}
