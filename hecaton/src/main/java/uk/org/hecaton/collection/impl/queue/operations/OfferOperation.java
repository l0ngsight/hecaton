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

package uk.org.hecaton.collection.impl.queue.operations;

import uk.org.hecaton.collection.impl.queue.QueueContainer;
import uk.org.hecaton.collection.impl.queue.QueueDataSerializerHook;
import uk.org.hecaton.core.ItemEventType;
import uk.org.hecaton.internal.monitor.impl.LocalQueueStatsImpl;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Notifier;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;

/**
 * Contains offer operation for the Queue.
 */
public final class OfferOperation extends QueueBackupAwareOperation
        implements BlockingOperation, Notifier, IdentifiedDataSerializable, MutatingOperation {

    private Data data;
    private long itemId;

    public OfferOperation() {
    }

    public OfferOperation(final String name, final long timeout, final Data data) {
        super(name, timeout);
        this.data = data;
    }

    @Override
    public void run() {
        QueueContainer queueContainer = getContainer();
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        if (queueContainer.hasEnoughCapacity()) {
            itemId = queueContainer.offer(data);
            stats.incrementOffers();
            response = true;
        } else {
            stats.incrementRejectedOffers();
            response = false;
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            publishEvent(ItemEventType.ADDED, data);
        }
    }

    @Override
    public Operation getBackupOperation() {
        return new OfferBackupOperation(name, data, itemId);
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getContainer().getPollWaitNotifyKey();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return getContainer().getOfferWaitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        QueueContainer container = getContainer();
        return getWaitTimeout() != 0 && !container.hasEnoughCapacity();
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.OFFER;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, data);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        data = IOUtil.readData(in);
    }
}
